// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

const Product = "codex-peer"
const EndpointEnv = "SESSIONBUS_CODEX_ENDPOINT"

var laneCommand = exec.Command

type Wrapper struct {
	caller            *sessionkit.Caller
	mu                sync.Mutex
	child             *nativeChild
	app               *appClient
	id, model, effort string
	approval          string
	sandbox           string
	active            *turn
	run               *sessionkit.Run
	boundary          uint64
	closing           bool
	shutdown          func()
	ctx               context.Context
	cancel            context.CancelFunc
	opened            bool
	failure           error
	endpoint          *laneEndpoint
	startup           map[string]string
	startupChanged    chan struct{}
}

type turn struct {
	owner    *Wrapper
	id       string
	started  bool
	ready    chan error
	done     chan error
	observed nativeTurn
}

type nativeTurn struct {
	ID          string       `json:"id"`
	Status      string       `json:"status"`
	Error       *appError    `json:"error"`
	Items       []nativeItem `json:"items"`
	CompletedAt *int64       `json:"completedAt"`
}

type nativeItem struct{ Type, Text, Phase string }
type nativeThread struct {
	ID, Name, Cwd, Path string
	Status              json.RawMessage
}
type threadReply struct {
	Thread         nativeThread   `json:"thread"`
	Cwd            string         `json:"cwd"`
	ApprovalPolicy nativeApproval `json:"approvalPolicy"`
	Sandbox        struct {
		Type string `json:"type"`
	} `json:"sandbox"`
}

// Native can report a granular approval object inherited from real config.
// Preserve it without substituting a policy; typed string requests stay strings.
type nativeApproval string

func (p *nativeApproval) UnmarshalJSON(raw []byte) error {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		*p = nativeApproval(text)
		return nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || len(object) == 0 {
		return errors.New("invalid native approval policy")
	}
	*p = nativeApproval(string(raw))
	return nil
}

type turnReply struct {
	Turn nativeTurn `json:"turn"`
}

func New() *Wrapper {
	return &Wrapper{startup: map[string]string{}, startupChanged: make(chan struct{})}
}
func (p *Wrapper) SetShutdown(shutdown func()) { p.shutdown = shutdown }
func (p *Wrapper) SetCall(call func(context.Context, string, any) (json.RawMessage, error)) {
	p.caller = sessionkit.NewCaller(call)
}

func (p *Wrapper) SetCaller(c *sessionkit.Caller) { p.caller = c }

func (*Wrapper) Hello(context.Context) (sessionkit.HelloDescription, error) {
	return sessionkit.HelloDescription{
		Product: Product, SupportsMessageRun: true, SupportedOpenFields: []string{"cwd", "permission_mode", "model", "reasoning_effort", "arguments"},
		ExtraArguments: []sessionkit.ExtraArgument{},
	}, nil
}

func (p *Wrapper) Open(ctx context.Context, request sessionkit.OpenRequest) (result sessionkit.OpenResult, err error) {
	stopStartup, err := p.startLifetime(ctx)
	if err != nil {
		return result, err
	}
	defer stopStartup()
	defer func() {
		if err != nil {
			_ = p.Close(context.Background(), sessionkit.SessionCloseRequest{})
		}
	}()
	nativeCtx := p.ctx
	if err = validateLaneTypedArguments(request.Open.Arguments, request.Open.Model, request.Open.ReasoningEffort); err != nil {
		return sessionkit.OpenResult{}, err
	}
	arguments, nativeBypass, err := laneArguments(request.Open.Arguments)
	if err != nil {
		return sessionkit.OpenResult{}, err
	}
	approval, sandbox, err := permission(request.Open.PermissionMode, nativeBypass)
	if err != nil {
		return sessionkit.OpenResult{}, err
	}
	effectiveCwd, err := filepath.Abs(first(request.Open.Cwd, "."))
	if err != nil {
		return sessionkit.OpenResult{}, fmt.Errorf("resolve Codex cwd: %w", err)
	}
	if _, err = namePart(request.Name); err != nil {
		return result, err
	}

	if p.caller == nil {
		return result, errors.New("Sessionbus lane caller unavailable")
	}
	endpoint, err := newLaneEndpoint(p)
	if err != nil {
		return result, err
	}
	p.mu.Lock()
	p.endpoint = endpoint
	p.mu.Unlock()
	command := laneCommand("codex", append(append([]string{"app-server", "--stdio"}, ActivationArguments()...), arguments...)...)
	command.Dir, command.Stderr = request.Open.Cwd, os.Stderr
	command.Env = slices.DeleteFunc(os.Environ(), func(value string) bool { return strings.HasPrefix(value, EndpointEnv+"=") })
	command.Env = append(command.Env, EndpointEnv+"="+endpoint.path)
	command.Env = slices.DeleteFunc(command.Env, func(value string) bool {
		key, _, _ := strings.Cut(value, "=")
		return slices.Contains([]string{host.TokenEnv, host.LocalKeyEnv, host.SocketEnv, host.SessionIDEnv, host.NameEnv, host.GroupsEnv}, key)
	})
	child, input, output, err := startNative(command)
	if err != nil {
		return sessionkit.OpenResult{}, fmt.Errorf("start Codex App Server: %w", err)
	}
	p.mu.Lock()
	p.child, p.model, p.effort, p.approval, p.sandbox = child, request.Open.Model, request.Open.ReasoningEffort, approval, sandbox
	p.app = newAppClient(input, output, p.receive, p.nativeFailure, p.serverRequest)
	p.mu.Unlock()
	stopNative := context.AfterFunc(nativeCtx, child.abort)
	go func() { <-child.Done(); stopNative() }()
	go p.watch(child, p.app.done)
	threadID := request.ResumeSessionID
	fresh := threadID == ""
	cleanup := func(err error) (sessionkit.OpenResult, error) { return sessionkit.OpenResult{}, err }
	if err = p.app.initialize(nativeCtx, "Sessionbus Codex Wrapper"); err != nil {
		return cleanup(err)
	}
	config := map[string]any{}
	if fresh {
		var started threadReply
		params := p.threadParams(request.Open.Cwd, config)
		params["ephemeral"], params["serviceName"], params["historyMode"] = false, Product, "legacy"
		if err = p.app.call(nativeCtx, "thread/start", params, &started); err != nil {
			return cleanup(err)
		}
		threadID = strings.TrimSpace(started.Thread.ID)
		if threadID == "" {
			return cleanup(errors.New("Codex App Server returned an empty thread id"))
		}
		if err = p.checkEffective("thread/start", effectiveCwd, started); err != nil {
			return cleanup(err)
		}
		name, nameErr := namePart(request.Name)
		if nameErr != nil {
			return cleanup(nameErr)
		}
		if err = p.app.call(nativeCtx, "thread/name/set", map[string]string{"threadId": threadID, "name": name}, &struct{}{}); err != nil {
			return cleanup(err)
		}
	}
	params := p.threadParams(request.Open.Cwd, config)
	params["threadId"], params["excludeTurns"] = threadID, true
	var resumed threadReply
	if err = p.app.call(nativeCtx, "thread/resume", params, &resumed); err != nil {
		return cleanup(err)
	}
	if resumed.Thread.ID != threadID {
		return cleanup(fmt.Errorf("Codex App Server changed native thread from %q to %q", threadID, resumed.Thread.ID))
	}
	if err = p.checkEffective("thread/resume", effectiveCwd, resumed); err != nil {
		return cleanup(err)
	}
	p.mu.Lock()
	p.id = threadID
	p.mu.Unlock()
	if err = p.awaitTools(nativeCtx, threadID); err != nil {
		return result, err
	}
	return p.commitOpen(ctx, request, resumed.Thread, stopStartup)
}

func (p *Wrapper) commitOpen(ctx context.Context, request sessionkit.OpenRequest, thread nativeThread, stopStartup func() bool) (sessionkit.OpenResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return sessionkit.OpenResult{}, err
	}
	if p.ctx.Err() != nil || p.closing || p.failure != nil {
		return sessionkit.OpenResult{}, errors.New("Codex integration ended during open")
	}
	name, err := namePart(request.Name)
	if err != nil {
		return sessionkit.OpenResult{}, err
	}
	if thread.Name != name {
		return sessionkit.OpenResult{}, errors.New("native title did not confirm requested name")
	}
	stopStartup()
	p.opened = true
	return sessionkit.OpenResult{SessionID: p.id}, nil
}

func (p *Wrapper) checkEffective(method, cwd string, reply threadReply) error {
	if reply.ApprovalPolicy == "" {
		return errors.New(method + " did not report its effective approval policy")
	}
	if p.approval != "" && string(reply.ApprovalPolicy) != p.approval {
		return fmt.Errorf("%s applied approval policy %q, expected %q", method, reply.ApprovalPolicy, p.approval)
	}
	if strings.TrimSpace(reply.Cwd) == "" {
		return errors.New(method + " did not report its effective cwd")
	}
	if reply.Cwd != cwd {
		return fmt.Errorf("%s applied cwd %q, expected %q", method, reply.Cwd, cwd)
	}
	if reply.Sandbox.Type == "" {
		return fmt.Errorf("%s reported unsupported sandbox %q", method, reply.Sandbox.Type)
	}
	if p.sandbox != "" && reply.Sandbox.Type != "dangerFullAccess" {
		return fmt.Errorf("%s applied sandbox %q, expected %q", method, reply.Sandbox.Type, "dangerFullAccess")
	}
	return nil
}

func (p *Wrapper) threadParams(cwd string, config map[string]any) map[string]any {
	params := map[string]any{"config": config}
	if p.approval != "" {
		params["approvalPolicy"] = p.approval
	}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if p.model != "" {
		params["model"] = p.model
	}
	if p.sandbox != "" {
		params["sandbox"] = p.sandbox
	}
	return params
}

func (p *Wrapper) Run(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (sessionkit.TurnResult, error) {
	return p.executeRun(ctx, run, seed, run.ReportDelivery)
}
func (p *Wrapper) executeRun(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput, report func(sessionkit.DeliveryReceipt, error) error) (sessionkit.TurnResult, error) {
	reject := func(err error) (sessionkit.TurnResult, error) {
		if seed.Delivery != nil {
			if reportErr := report(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil); reportErr != nil {
				return sessionkit.TurnResult{}, reportErr
			}
		}
		return sessionkit.TurnResult{}, err
	}
	if (seed.Text == nil) == (seed.Delivery == nil) {
		return reject(errors.New("expected exactly one run input"))
	}
	if err := ctx.Err(); err != nil {
		return reject(err)
	}
	if run.Interrupted() {
		return reject(errors.New("run interrupted before native submission"))
	}
	var input string
	if seed.Text != nil {
		input = *seed.Text
	} else {
		var err error
		input, err = host.RenderNativeMessage(*seed.Delivery)
		if err != nil {
			return reject(err)
		}
	}
	p.mu.Lock()
	p.run = run
	p.mu.Unlock()
	go p.retireRun(run)
	native, err := p.start(ctx, input)
	if err != nil {
		if seed.Delivery != nil {
			if reportErr := report(sessionkit.DeliveryReceipt{}, uncertainAdmission(err)); reportErr != nil {
				return sessionkit.TurnResult{}, reportErr
			}
		}
		return sessionkit.TurnResult{}, err
	}
	run.Admitted()
	// Public receipt I/O cannot block the native reader or terminal correlation.
	if seed.Delivery != nil {
		if err = report(sessionkit.DeliveryReceipt{Disposition: "injected"}, nil); err != nil {
			return sessionkit.TurnResult{}, err
		}
	}
	if run.Interrupted() {
		if err = native.Interrupt(ctx); err != nil {
			return sessionkit.TurnResult{}, err
		}
	}
	return native.Wait(ctx)
}
func (p *Wrapper) retireRun(run *sessionkit.Run) {
	<-run.Done()
	p.mu.Lock()
	if p.run == run {
		p.run = nil
	}
	p.mu.Unlock()
}
func (p *Wrapper) Interrupt(ctx context.Context, run *sessionkit.Run) error {
	p.mu.Lock()
	t := p.active
	matching := p.run == run
	p.mu.Unlock()
	if !matching || t == nil {
		return nil
	}
	p.mu.Lock()
	started := t.started
	p.mu.Unlock()
	// The Run path sees Interrupted after native start if admission is pending.
	if !started {
		return nil
	}
	return t.Interrupt(ctx)
}
func uncertainAdmission(cause error) error {
	return &sessionkit.ProtocolError{Code: -32603, Message: cause.Error(), Data: json.RawMessage(`"uncertain_native_admission"`)}
}
func (p *Wrapper) Deliver(ctx context.Context, request sessionkit.DeliveryRequest, _ *sessionkit.Run) (sessionkit.DeliveryReceipt, error) {
	body, err := host.RenderNativeMessage(request)
	if err != nil {
		return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: err.Error()}, nil
	}
	if err = ctx.Err(); err != nil {
		return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil
	}
	p.mu.Lock()
	active := p.active
	expectedTurnID := ""
	if active != nil && active.started {
		expectedTurnID = active.id
	}
	p.mu.Unlock()
	if expectedTurnID == "" {
		return sessionkit.DeliveryReceipt{}, host.NotRunning()
	}
	outcome, err := p.injectExpected(ctx, body, active)
	if err != nil {
		if codexSteerDefinitelyNotSubmitted(err, expectedTurnID) {
			return sessionkit.DeliveryReceipt{}, host.NotRunning()
		}
		return sessionkit.DeliveryReceipt{}, uncertainAdmission(err)
	}
	if outcome != host.Injected {
		return sessionkit.DeliveryReceipt{}, host.NotRunning()
	}
	return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
}

// codexSteerDefinitelyNotSubmitted recognizes only the two stable 0.153.4
// turn/steer errors produced from SteerSubmission::NotSubmitted when the
// locally observed turn has already ended or changed. Other native, transport,
// policy, cancellation, and schema failures remain uncertain after the RPC.
func codexSteerDefinitelyNotSubmitted(err error, expectedTurnID string) bool {
	var protocolError *sessionkit.ProtocolError
	if !errors.As(err, &protocolError) || protocolError.Code != -32600 {
		return false
	}
	if protocolError.Message == "no active turn to steer" {
		return true
	}
	prefix := fmt.Sprintf("expected active turn id `%s` but found `", expectedTurnID)
	return strings.HasPrefix(protocolError.Message, prefix) && strings.HasSuffix(protocolError.Message, "`")
}

func (p *Wrapper) start(ctx context.Context, prompt string) (host.Turn, error) {
	p.mu.Lock()
	if p.active != nil {
		p.mu.Unlock()
		return nil, errors.New("Codex lane is not idle")
	}
	t := &turn{owner: p, ready: make(chan error, 1), done: make(chan error, 1)}
	p.active = t
	p.boundary++
	params := map[string]any{"threadId": p.id, "input": textInput(prompt)}
	if p.approval != "" {
		params["approvalPolicy"] = p.approval
	}
	if p.model != "" {
		params["model"] = p.model
	}
	if p.effort != "" {
		params["effort"] = p.effort
	}
	if p.sandbox != "" {
		params["sandboxPolicy"] = map[string]string{"type": "dangerFullAccess"}
	}
	p.mu.Unlock()
	var started turnReply
	if err := p.app.call(ctx, "turn/start", params, &started); err != nil {
		p.clear(t)
		return nil, err
	}
	if started.Turn.ID == "" {
		p.clear(t)
		return nil, errors.New("Codex App Server returned an empty turn id")
	}
	p.mu.Lock()
	if t.id != "" && t.id != started.Turn.ID {
		observed := t.id
		p.mu.Unlock()
		p.clear(t)
		return nil, fmt.Errorf("Codex turn/start returned %q after starting %q", started.Turn.ID, observed)
	}
	t.id = started.Turn.ID
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		p.clear(t)
		return nil, ctx.Err()
	case err := <-t.ready:
		if err != nil {
			return nil, err
		}
		return t, nil
	}
}

func (p *Wrapper) injectExpected(ctx context.Context, prompt string, expected *turn) (host.Injection, error) {
	p.mu.Lock()
	t := p.active
	if t == nil || t != expected || !t.started {
		p.mu.Unlock()
		return host.NotInjected, nil
	}
	threadID, turnID := p.id, t.id
	p.mu.Unlock()
	var result struct {
		TurnID string `json:"turnId"`
	}
	err := p.app.call(ctx, "turn/steer", map[string]any{"threadId": threadID, "expectedTurnId": turnID, "input": textInput(prompt)}, &result)
	if err != nil {
		return host.NotInjected, err
	}
	if result.TurnID != turnID {
		return host.NotInjected, fmt.Errorf("Codex App Server steered turn %q, expected %q", result.TurnID, turnID)
	}
	return host.Injected, nil
}

func (t *turn) Wait(ctx context.Context) (sessionkit.TurnResult, error) {
	select {
	case <-ctx.Done():
		return sessionkit.TurnResult{}, ctx.Err()
	case err := <-t.done:
		if err != nil {
			return sessionkit.TurnResult{}, err
		}
		if result, resultErr, closed := t.closedResult(); closed {
			return result, resultErr
		}
		result, resultErr := t.read(ctx)
		if resultErr != nil {
			if observed, observedErr, closed := t.closedResult(); closed {
				return observed, observedErr
			}
		}
		return result, resultErr
	}
}

func (t *turn) closedResult() (sessionkit.TurnResult, error, bool) {
	t.owner.mu.Lock()
	observed, child, app := t.observed, t.owner.child, t.owner.app
	t.owner.mu.Unlock()
	closed := app != nil && app.isFailed()
	if !closed && child != nil {
		select {
		case <-child.Done():
			closed = true
		default:
		}
	}
	if !closed {
		return sessionkit.TurnResult{}, nil, false
	}
	result, err := terminal(observed)
	return result, err, true
}

func (t *turn) read(ctx context.Context) (sessionkit.TurnResult, error) {
	var page struct {
		Data []nativeTurn `json:"data"`
	}
	if err := t.owner.app.call(ctx, "thread/turns/list", map[string]any{
		"threadId": t.owner.id, "limit": 100, "sortDirection": "desc", "itemsView": "full",
	}, &page); err != nil {
		return sessionkit.TurnResult{}, err
	}
	for _, candidate := range page.Data {
		if candidate.ID == t.id {
			return terminal(candidate)
		}
	}
	return sessionkit.TurnResult{}, fmt.Errorf("Codex turn %s is absent from thread %s", t.id, t.owner.id)
}
func (t *turn) Interrupt(ctx context.Context) error {
	return t.owner.app.call(ctx, "turn/interrupt", map[string]string{"threadId": t.owner.id, "turnId": t.id}, &struct{}{})
}

func (p *Wrapper) receive(method string, raw json.RawMessage) {
	if method == "mcpServer/startupStatus/updated" {
		p.receiveStartup(raw)
		return
	}
	if method != "turn/started" && method != "turn/completed" {
		return
	}
	var event struct {
		ThreadID string     `json:"threadId"`
		Turn     nativeTurn `json:"turn"`
	}
	if json.Unmarshal(raw, &event) != nil || event.ThreadID == "" || event.Turn.ID == "" {
		p.fail(errors.New("malformed Codex App Server " + method + " notification"))
		return
	}
	p.mu.Lock()
	t := p.active
	if event.ThreadID == p.id {
		p.boundary++
	}
	if t == nil || event.ThreadID != p.id {
		p.mu.Unlock()
		return
	}
	if t.id != "" && event.Turn.ID != t.id {
		expected := t.id
		p.mu.Unlock()
		p.fail(fmt.Errorf("Codex App Server reported turn %q while waiting for %q", event.Turn.ID, expected))
		return
	}
	if t.id == "" && (method == "turn/started" || method == "turn/completed") {
		t.id = event.Turn.ID
	}
	switch method {
	case "turn/started":
		if t.id == "" {
			t.id = event.Turn.ID
		}
		if event.Turn.ID == t.id && !t.started {
			t.started = true
			t.ready <- nil
		}
	case "turn/completed":
		if event.Turn.ID == t.id {
			if !t.started {
				t.started = true
				t.ready <- nil
			}
			t.observed = event.Turn
			p.active = nil
			t.done <- nil
		}
	}
	p.mu.Unlock()
}

func terminal(t nativeTurn) (sessionkit.TurnResult, error) {
	if t.CompletedAt == nil {
		return sessionkit.TurnResult{}, fmt.Errorf("Codex turn %s is not product-closed", t.ID)
	}
	result := sessionkit.TurnResult{Outcome: t.Status, NativeStopReason: t.Status}
	answers := make([]string, 0, len(t.Items))
	for _, item := range t.Items {
		// Steering can leave more than one final answer in the persisted turn.
		// Exact empty items carry no text; whitespace-only text remains content.
		if item.Type == "agentMessage" && item.Phase == "final_answer" && item.Text != "" {
			answers = append(answers, item.Text)
		}
	}
	result.Result = strings.Join(answers, "\n\n")
	switch t.Status {
	case "completed":
		result.Outcome = "completed"
		if len(answers) == 0 {
			return sessionkit.TurnResult{}, fmt.Errorf("completed Codex turn %s has no final answer", t.ID)
		}
	case "interrupted":
		result.Outcome = "interrupted"
	case "failed":
		result.Outcome = "failed"
		if t.Error != nil {
			result.Result = t.Error.Message
		}
		if result.Result == "" {
			result.Result = "Codex turn failed"
		}
	default:
		return sessionkit.TurnResult{}, fmt.Errorf("Codex turn %s has unsupported terminal status %q", t.ID, t.Status)
	}
	return result, nil
}

func (p *Wrapper) clear(t *turn) {
	p.mu.Lock()
	if p.active == t {
		p.active = nil
	}
	p.mu.Unlock()
}
func (p *Wrapper) fail(err error) {
	p.mu.Lock()
	if p.failure != nil {
		p.mu.Unlock()
		return
	}
	p.failure = err
	t := p.active
	p.active = nil
	cancel, opened, closing, run, shutdown := p.cancel, p.opened, p.closing, p.run, p.shutdown
	if t != nil {
		if !t.started {
			t.started = true
			t.ready <- err
		} else {
			t.done <- err
		}
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if opened && !closing && shutdown != nil {
		go func() {
			if run != nil {
				<-run.Done()
			}
			shutdown()
		}()
	}
}

func (p *Wrapper) watch(child *nativeChild, drained <-chan struct{}) {
	err := child.Wait()
	<-drained
	if err == nil {
		err = errors.New("Codex App Server exited")
	}
	p.transportEnd(err)
	p.mu.Lock()
	opened, closing, run := p.id != "", p.closing, p.run
	p.mu.Unlock()
	if opened && !closing {
		if run != nil {
			<-run.Done()
		}
		if p.shutdown != nil {
			p.shutdown()
		}
	}
}

func (p *Wrapper) Close(ctx context.Context, _ sessionkit.SessionCloseRequest) error {
	p.mu.Lock()
	p.closing = true
	child, app, endpoint, cancel := p.child, p.app, p.endpoint, p.cancel
	graceful := p.opened && p.failure == nil && ctx.Err() == nil
	var cleanupErr error
	p.mu.Unlock()
	if graceful && app != nil {
		if err := app.close(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			p.fail(err)
			graceful = false
		}
	}
	if !graceful && cancel != nil {
		cancel()
	}
	if child != nil {
		var drained <-chan struct{}
		if app != nil {
			drained = app.done
		}
		done := child.Done()
		for done != nil || drained != nil {
			select {
			case <-done:
				done = nil
			case <-drained:
				drained = nil
			case <-ctx.Done():
				if cancel != nil {
					cancel()
				}
				child.abort()
				_ = child.Wait()
				if app != nil {
					app.fail(ctx.Err())
					<-app.done
				}
				done = nil
				drained = nil
			}
		}
	}
	if child != nil {
		cleanupErr = errors.Join(cleanupErr, child.Wait())
	}
	if app != nil {
		app.mu.Lock()
		drainErr := app.failed
		app.mu.Unlock()
		if drainErr != nil && !errors.Is(drainErr, io.EOF) {
			cleanupErr = errors.Join(cleanupErr, drainErr)
		}
	}
	if cancel != nil {
		cancel()
	}
	if endpoint != nil {
		cleanupErr = errors.Join(cleanupErr, endpoint.Close())
	}
	return errors.Join(cleanupErr, ctx.Err())
}

func textInput(text string) []map[string]string {
	return []map[string]string{{"type": "text", "text": text}}
}
func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
