// SPDX-License-Identifier: MIT
package codex

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/testsocket"
)

type signaledBrokerWriteConn struct {
	net.Conn
	once     sync.Once
	entered  chan struct{}
	released chan struct{}
}

type gatedBrokerWriteConn struct {
	net.Conn
	enabled atomic.Bool
	held    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *gatedBrokerWriteConn) Write(body []byte) (int, error) {
	if c.enabled.Load() && c.held.CompareAndSwap(false, true) {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(body)
}

func (c *signaledBrokerWriteConn) Write(body []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	n, err := c.Conn.Write(body)
	close(c.released)
	return n, err
}

func residentFixture(t *testing.T) (*brokerOwners, *brokerMux, *muxPipe, <-chan *muxPipe) {
	t.Helper()
	o := newBrokerOwners(context.Background(), []string{"launch"}, "/bus")
	buses := make(chan *muxPipe, 8)
	o.dial = func(context.Context, string, string) (net.Conn, error) {
		a, b := net.Pipe()
		p := &muxPipe{Conn: b, dec: json.NewDecoder(b)}
		buses <- p
		t.Cleanup(func() { _ = p.Close() })
		return a, nil
	}
	m, p := muxFixture(t, o.observe)
	o.setMux(m)
	t.Cleanup(o.End)
	initializeMux(t, m, p)
	return o, m, p, buses
}
func nativeEvent(t *testing.T, p *muxPipe, m *brokerMux, method string, params any) {
	t.Helper()
	if err := p.Write(frame(method, nil, params)); err != nil {
		t.Fatal(err)
	}
	receiveTUI(t, m)
}
func loaded(t *testing.T, o *brokerOwners, m *brokerMux, p *muxPipe, id string) {
	t.Helper()
	nativeEvent(t, p, m, "thread/started", map[string]any{"thread": map[string]any{"id": id, "cwd": "/real", "status": map[string]string{"type": "idle"}}})
}
func readyCatalog(t *testing.T, m *brokerMux, p *muxPipe, id string) {
	t.Helper()
	nativeEvent(t, p, m, "mcpServer/startupStatus/updated", map[string]string{"threadId": id, "name": laneServer, "status": "ready"})
	request := receiveNative(t, p)
	if string(request["method"]) != `"mcpServerStatus/list"` {
		t.Fatal(request)
	}
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": request["id"], "result": brokerRaw(map[string]any{"data": []any{map[string]any{"name": laneServer, "pluginId": PluginID, "runtimeStatus": "connected", "tools": map[string]any{"sessionbus": map[string]any{}}}}})}); err != nil {
		t.Fatal(err)
	}
}
func receiveBus(t *testing.T, buses <-chan *muxPipe) *muxPipe {
	t.Helper()
	select {
	case p := <-buses:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("resident did not connect")
	}
	return nil
}
func acceptHello(t *testing.T, p *muxPipe) brokerFrame {
	t.Helper()
	hello := receiveNative(t, p)
	if string(hello["method"]) != `"session.hello"` {
		t.Fatal(hello)
	}
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": hello["id"], "result": brokerRaw(map[string]any{})}); err != nil {
		t.Fatal(err)
	}
	return hello
}
func residentBarrier(t *testing.T, p *muxPipe) {
	t.Helper() // A same-reader inbound request must follow the hello observer.
	if err := p.Write(frame("message.deliver", 2, kit.DeliveryRequest{MessageID: "preflight", From: kit.DeliverySource{SessionID: "source@local", Product: "fixture", Groups: []string{"launch"}}, Body: "x"})); err != nil {
		t.Fatal(err)
	}
}
func gateResidentRetries(o *brokerOwners) (<-chan struct{}, chan<- struct{}) {
	waiting := make(chan struct{})
	release := make(chan struct{})
	o.retry = func(ctx context.Context) bool {
		select {
		case waiting <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		select {
		case <-release:
			return true
		case <-ctx.Done():
			return false
		}
	}
	return waiting, release
}
func completeIdleResidentDelivery(t *testing.T, bus, native *muxPipe) {
	t.Helper()
	residentBarrier(t, bus)
	// The receipt is the message reaching this product; it does not wait for the native.
	receipt := receiveNative(t, bus)
	if string(receipt["id"]) != "2" || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatal(receipt)
	}
	read := receiveNative(t, native)
	if string(read["method"]) != `"thread/read"` {
		t.Fatal(read)
	}
	if err := native.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": read["id"], "result": brokerRaw(map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "idle"}}})}); err != nil {
		t.Fatal(err)
	}
	turn := receiveNative(t, native)
	if string(turn["method"]) != `"turn/start"` {
		t.Fatal(turn)
	}
	if err := native.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": turn["id"], "result": brokerRaw(map[string]any{"turn": map[string]string{"id": "barrier-turn"}})}); err != nil {
		t.Fatal(err)
	}
}
func TestBrokerResidentIdentityReadyRenameAndClosed(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	for _, meta := range []string{`{}`, `{"threadId":"unknown"}`} {
		if _, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(meta)); err == nil {
			t.Fatal("unsettled identity accepted")
		}
	}
	// Early startup is bound to this exact thread; no helper-spawn attribution.
	readyCatalogBefore := func() {
		nativeEvent(t, p, m, "mcpServer/startupStatus/updated", map[string]string{"threadId": "one", "name": laneServer, "status": "ready"})
	}
	readyCatalogBefore()
	loaded(t, o, m, p, "one")
	request := receiveNative(t, p)
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": request["id"], "result": brokerRaw(map[string]any{"data": []any{map[string]any{"name": laneServer, "pluginId": PluginID, "runtimeStatus": "connected", "tools": map[string]any{"sessionbus": map[string]any{}}}}})}); err != nil {
		t.Fatal(err)
	}
	bus := receiveBus(t, buses)
	hello := acceptHello(t, bus)
	var identity kit.Identity
	if err := json.Unmarshal(hello["params"], &identity); err != nil {
		t.Fatal(err)
	}
	if identity.SessionID != "one" || identity.Name != "" || len(identity.Groups) != 1 || identity.Groups[0] != "launch" {
		t.Fatal(identity)
	}
	// Rename and clear keep the same connection and native ID.
	for _, name := range []any{"new title", nil} {
		nativeEvent(t, p, m, "thread/name/updated", map[string]any{"threadId": "one", "threadName": name})
		h := acceptHello(t, bus)
		var value map[string]any
		_ = json.Unmarshal(h["params"], &value)
		if value["session_id"] != "one" || value["name"] != name {
			t.Fatal(value)
		}
	}
	nativeEvent(t, p, m, "thread/closed", map[string]string{"threadId": "one"})
	var frame brokerFrame
	_ = bus.SetReadDeadline(time.Now().Add(time.Second))
	if err := bus.Read(&frame); err == nil {
		t.Fatal("closed thread retained connection")
	}
	if _, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`)); err == nil {
		t.Fatal("closed thread accepted action")
	}
}
func TestBrokerResidentToolAndDeliveryMakeProgressTogether(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	acceptHello(t, bus)
	// A delivery after hello is processed without waiting on a public tool call.
	residentBarrier(t, bus)
	receipt := receiveNative(t, bus)
	if string(receipt["id"]) != "2" || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatal(receipt)
	}
	read := receiveNative(t, p)
	if string(read["method"]) != `"thread/read"` {
		t.Fatal(read)
	}
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": read["id"], "result": brokerRaw(map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "idle"}}})}); err != nil {
		t.Fatal(err)
	}
	turn := receiveNative(t, p)
	if string(turn["method"]) != `"turn/start"` {
		t.Fatal(turn)
	}
	action := make(chan error, 1)
	go func() {
		_, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one","preserved":"native"}`))
		action <- err
	}()
	public := receiveNative(t, bus)
	if string(public["method"]) != `"session.list"` {
		t.Fatal(public)
	}
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": turn["id"], "result": brokerRaw(map[string]any{"turn": map[string]string{"id": "new-turn"}})}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": public["id"], "result": brokerRaw(map[string]any{"sessions": []any{}})}); err != nil {
		t.Fatal(err)
	}
	if err := <-action; err != nil {
		t.Fatal(err)
	}
	// Ending the native broker remains terminal and joins the resident publisher.
	_ = bus.Close()
	o.End()
	select {
	case unexpected := <-buses:
		_ = unexpected.Close()
		t.Fatal("unexpected reconnect")
	default:
	}
}

func TestBrokerResidentReconnectsLatestIdentityAndDoesNotReplayLostCall(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	waiting, retry := gateResidentRetries(o)
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	acceptHello(t, bus)
	completeIdleResidentDelivery(t, bus, p)
	o.mu.Lock()
	resident := o.owners["one"]
	o.mu.Unlock()
	resident.mu.Lock()
	oldConnection := resident.connection
	resident.mu.Unlock()

	returned := make(chan error, 1)
	go func() {
		_, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`))
		returned <- err
	}()
	lost := receiveNative(t, bus)
	if string(lost["method"]) != `"session.list"` {
		t.Fatal(lost)
	}
	_ = bus.Close()
	if err := <-returned; err == nil {
		t.Fatal("lost admitted call succeeded")
	}
	<-waiting
	if _, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`)); err == nil {
		t.Fatal("outage action succeeded")
	}
	nativeEvent(t, p, m, "thread/name/updated", map[string]any{"threadId": "one", "threadName": "renamed while down"})
	retry <- struct{}{}
	next := receiveBus(t, buses)
	hello := acceptHello(t, next)
	var identity kit.Identity
	if err := json.Unmarshal(hello["params"], &identity); err != nil {
		t.Fatal(err)
	}
	if identity.SessionID != "one" || identity.Name != "renamed while down" || identity.Info["cwd"] != "/real" || len(identity.Groups) != 1 || identity.Groups[0] != "launch" {
		t.Fatal(identity)
	}
	// A same-reader delivery after the hello response proves admission and also
	// proves that the lost session.list was not replayed on the fresh wire.
	completeIdleResidentDelivery(t, next, p)
	resident.handle(oldConnection, &kit.Request{ID: 99, Method: "session.superseded", Params: &struct{}{}})
	go func() {
		_, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`))
		returned <- err
	}()
	fresh := receiveNative(t, next)
	if string(fresh["method"]) != `"session.list"` {
		t.Fatal(fresh)
	}
	if err := next.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": fresh["id"], "result": brokerRaw(map[string]any{"sessions": []any{}})}); err != nil {
		t.Fatal(err)
	}
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
}

func TestBrokerOwnerEndJoinsCanceledDialAndHelloWrite(t *testing.T) {
	for _, phase := range []string{"dial", "hello-write"} {
		t.Run(phase, func(t *testing.T) {
			o, m, p, _ := residentFixture(t)
			entered, released := make(chan struct{}), make(chan struct{})
			var peer net.Conn
			o.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
				if phase == "dial" {
					close(entered)
					<-ctx.Done()
					close(released)
					return nil, ctx.Err()
				}
				a, b := net.Pipe()
				peer = b
				return &signaledBrokerWriteConn{Conn: a, entered: entered, released: released}, nil
			}
			loaded(t, o, m, p, "one")
			readyCatalog(t, m, p, "one")
			<-entered
			done := make(chan struct{})
			go func() { o.End(); close(done) }()
			<-released
			<-done
			if peer != nil {
				_ = peer.Close()
			}
		})
	}
}

func TestBrokerInvalidHelloIsTerminalWithoutReconnect(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	waiting, _ := gateResidentRetries(o)
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	hello := receiveNative(t, bus)
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": hello["id"], "error": brokerRaw(map[string]any{"code": protocol.InvalidHello, "message": "invalid_hello"})}); err != nil {
		t.Fatal(err)
	}
	o.work.Wait()
	o.End()
	select {
	case <-waiting:
		t.Fatal("invalid hello entered reconnect backoff")
	default:
	}
	if _, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`)); err == nil {
		t.Fatal("invalid hello resident accepted action")
	}
}

func TestBrokerResidentRecoversWhenDaemonAbsentAtStartup(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	waiting, retry := gateResidentRetries(o)
	originalDial := o.dial
	first := true
	o.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		if first {
			first = false
			return nil, context.DeadlineExceeded
		}
		return originalDial(ctx, network, address)
	}
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	<-waiting
	retry <- struct{}{}
	bus := receiveBus(t, buses)
	hello := acceptHello(t, bus)
	var identity kit.Identity
	if err := json.Unmarshal(hello["params"], &identity); err != nil || identity.SessionID != "one" {
		t.Fatal(identity, err)
	}
}

func TestBrokerResidentHeldDialPublishesLatestGenerationOnly(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	originalDial := o.dial
	entered, release := make(chan struct{}), make(chan struct{})
	o.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return originalDial(ctx, network, address)
	}
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	<-entered
	nativeEvent(t, p, m, "thread/name/updated", map[string]any{"threadId": "one", "threadName": "latest"})
	close(release)
	bus := receiveBus(t, buses)
	hello := acceptHello(t, bus)
	var identity kit.Identity
	if err := json.Unmarshal(hello["params"], &identity); err != nil || identity.SessionID != "one" || identity.Name != "latest" {
		t.Fatal(identity, err)
	}
}

func TestBrokerSupersessionIsTerminalAndJoinsReconnectWork(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	waiting, _ := gateResidentRetries(o)
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	acceptHello(t, bus)
	completeIdleResidentDelivery(t, bus, p)
	if err := bus.Write(frame("session.superseded", 3, map[string]any{})); err != nil {
		t.Fatal(err)
	}
	if result := receiveNative(t, bus); string(result["id"]) != "3" || result["error"] != nil {
		t.Fatal(result)
	}
	o.End()
	select {
	case <-waiting:
		t.Fatal("superseded resident entered reconnect backoff")
	default:
	}
	if _, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`)); err == nil {
		t.Fatal("superseded resident accepted action")
	}
}

func TestBrokerSupersessionRetiresBeforeHeldAckAndLateCatalog(t *testing.T) {
	o, m, p, buses := residentFixture(t)
	originalDial := o.dial
	held := &gatedBrokerWriteConn{entered: make(chan struct{}), release: make(chan struct{})}
	o.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		fd, err := originalDial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		held.Conn = fd
		return held, nil
	}
	loaded(t, o, m, p, "one")
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	acceptHello(t, bus)

	// Keep a valid catalog result in flight while the public daemon withdraws
	// this exact resident. It must not republish the resident while the terminal
	// ACK write is held.
	nativeEvent(t, p, m, "mcpServer/startupStatus/updated", map[string]string{"threadId": "one", "name": laneServer, "status": "starting"})
	nativeEvent(t, p, m, "mcpServer/startupStatus/updated", map[string]string{"threadId": "one", "name": laneServer, "status": "ready"})
	catalog := receiveNative(t, p)
	if string(catalog["method"]) != `"mcpServerStatus/list"` {
		t.Fatal(catalog)
	}
	o.mu.Lock()
	resident := o.owners["one"]
	o.mu.Unlock()
	held.enabled.Store(true)
	if err := bus.Write(frame("session.superseded", 3, map[string]any{})); err != nil {
		t.Fatal(err)
	}
	<-held.entered
	select {
	case <-resident.ctx.Done():
	default:
		t.Fatal("superseded resident remained live while ACK was held")
	}
	resident.mu.Lock()
	if !resident.closed || resident.admitted || resident.tools {
		t.Fatalf("superseded resident remained publishable: closed=%v admitted=%v tools=%v", resident.closed, resident.admitted, resident.tools)
	}
	resident.mu.Unlock()
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": catalog["id"], "result": brokerRaw(map[string]any{"data": []any{map[string]any{"name": laneServer, "pluginId": PluginID, "runtimeStatus": "connected", "tools": map[string]any{"sessionbus": map[string]any{}}}}})}); err != nil {
		t.Fatal(err)
	}
	nativeEvent(t, p, m, "mcpServer/startupStatus/updated", map[string]string{"threadId": "one", "name": laneServer, "status": "ready"})
	close(held.release)
	if result := receiveNative(t, bus); string(result["id"]) != "3" || result["error"] != nil {
		t.Fatal(result)
	}
	o.End()
	resident.mu.Lock()
	defer resident.mu.Unlock()
	if resident.tools || resident.admitted || resident.connection != nil || resident.caller != nil {
		t.Fatal("late catalog result revived superseded resident")
	}
}
func TestBrokerInitialNameOnlyCorrelatedTUISelection(t *testing.T) {
	o, m, p, _ := residentFixture(t)
	o.initialName = "initial name"
	// Install the selection callback before sending any selection request.
	m.mu.Lock()
	m.selectThread = o.selectThread
	m.mu.Unlock()
	loaded(t, o, m, p, "subthread")
	if err := m.fromTUI(frame("thread/resume", 9, map[string]string{"threadId": "selected"})); err != nil {
		t.Fatal(err)
	}
	selection := receiveNative(t, p)
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": selection["id"], "result": brokerRaw(map[string]any{"thread": map[string]any{"id": "selected", "cwd": "/real"}})}); err != nil {
		t.Fatal(err)
	}
	receiveTUI(t, m)
	naming := receiveNative(t, p)
	var params map[string]string
	_ = json.Unmarshal(naming["params"], &params)
	if string(naming["method"]) != `"thread/name/set"` || params["threadId"] != "selected" || params["name"] != "initial name" {
		t.Fatal(naming)
	}
	if err := p.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": naming["id"], "result": brokerRaw(map[string]any{})}); err != nil {
		t.Fatal(err)
	}
	if err := o.selectThread("thread/fork", brokerRaw(map[string]any{"thread": map[string]string{"id": "later"}})); err != nil {
		t.Fatal(err)
	}
	if err := m.fromTUI(frame("thread/read", 10, map[string]string{"threadId": "selected"})); err != nil {
		t.Fatal(err)
	}
	if next := receiveNative(t, p); string(next["method"]) != `"thread/read"` {
		t.Fatal("renamed subsequent selection")
	}
}

func TestBrokerMCPForwarderCatalogEOFDoesNotWithdrawResident(t *testing.T) {
	o, m, p, _ := residentFixture(t)
	loaded(t, o, m, p, "loaded")
	endpoint, err := newBrokerEndpoint(testsocket.Directory(t), o)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	c, err := net.Dial("unix", endpoint.path)
	if err != nil {
		t.Fatal(err)
	}
	peer := &muxPipe{Conn: c, dec: json.NewDecoder(c)}
	defer peer.Close()
	for _, method := range []string{"initialize", "tools/list"} {
		params := map[string]any{}
		if method == "initialize" {
			params["protocolVersion"] = "2024-11-05"
		}
		if err := peer.Write(frame(method, 1, params)); err != nil {
			t.Fatal(err)
		}
		if reply := receiveNative(t, peer); reply["error"] != nil {
			t.Fatal(reply)
		}
	}
	if err := peer.Write(frame("tools/call", 2, map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}, "_meta": map[string]any{"threadId": "unknown"}})); err != nil {
		t.Fatal(err)
	}
	reply := receiveNative(t, peer)
	if !strings.Contains(string(reply["result"]), "not loaded") {
		t.Fatal(reply)
	}
	_ = c.Close()
	o.mu.Lock()
	resident := o.owners["loaded"]
	o.mu.Unlock()
	resident.mu.Lock()
	closed := resident.closed
	resident.mu.Unlock()
	if closed {
		t.Fatal("unidentified catalog connection withdrew native resident")
	}
}

func TestBrokerActiveDeliveryUsesExpectedNativeTurnAndNoReplay(t *testing.T) {
	for _, loss := range []bool{false, true} {
		t.Run(map[bool]string{false: "native-ack", true: "uncertain-write"}[loss], func(t *testing.T) {
			o, m, p, _ := residentFixture(t)
			loaded(t, o, m, p, "one")
			o.mu.Lock()
			resident := o.owners["one"]
			o.mu.Unlock()
			receipt, err := resident.accept(kit.DeliveryRequest{MessageID: "message", From: kit.DeliverySource{SessionID: "sender@local", Product: "fixture", Groups: []string{}}, Body: "native input"})
			if err != nil || receipt.Disposition != "written" {
				t.Fatal(receipt, err)
			}
			request := receiveNative(t, p)
			if string(request["method"]) != `"thread/read"` {
				t.Fatal(request)
			}
			if err := p.Write(brokerFrame{"id": request["id"], "result": brokerRaw(map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "active"}}})}); err != nil {
				t.Fatal(err)
			}
			turns := receiveNative(t, p)
			if string(turns["method"]) != `"thread/turns/list"` {
				t.Fatal(turns)
			}
			if err := p.Write(brokerFrame{"id": turns["id"], "result": brokerRaw(map[string]any{"data": []any{map[string]string{"id": "native-turn", "status": "inProgress"}}})}); err != nil {
				t.Fatal(err)
			}
			steer := receiveNative(t, p)
			var params map[string]any
			_ = json.Unmarshal(steer["params"], &params)
			if string(steer["method"]) != `"turn/steer"` || params["threadId"] != "one" || params["expectedTurnId"] != "native-turn" {
				t.Fatal(steer)
			}
			if loss {
				_ = p.Close()
			} else if err := p.Write(brokerFrame{"id": steer["id"], "result": brokerRaw(map[string]string{"turnId": "native-turn"})}); err != nil {
				t.Fatal(err)
			}
			// Acknowledged or uncertain, the message leaves the queue and no call remains or follows.
			for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(5 * time.Millisecond) {
				resident.mu.Lock()
				queued := len(resident.queue)
				resident.mu.Unlock()
				m.mu.Lock()
				remaining := len(m.calls)
				m.mu.Unlock()
				if queued == 0 && remaining == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("retained or replayed native delivery", queued, remaining)
				}
			}
		})
	}
}
