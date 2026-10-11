// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const brokerRouteLimit = 256
const brokerMessageLimit = 128 << 20

type brokerFrame map[string]json.RawMessage

type brokerPacket struct{ body json.RawMessage }

type brokerCall struct {
	original json.RawMessage
	method   string
	reply    chan appReply
	held     int
}
type brokerServerCall struct {
	original json.RawMessage
	held     int
}

// brokerMux owns both JSON-RPC ID domains. Its reader callbacks must only
// publish state; blocking native/bus calls run outside the reader.
type brokerMux struct {
	budgetMu                                  sync.Mutex
	buffered                                  int
	byteLimit                                 int
	ctx                                       context.Context
	cancel                                    context.CancelFunc
	native                                    appTransport
	mu                                        sync.Mutex
	next                                      uint64
	calls                                     map[string]*brokerCall
	tuiIDs                                    map[string]bool
	servers                                   map[string]*brokerServerCall
	nativeOut                                 chan brokerPacket
	tuiOut                                    chan brokerPacket
	ready                                     chan struct{}
	initialized, initializeSeen, initializeOK bool
	readyOnce                                 sync.Once
	err                                       error
	observe                                   func(string, json.RawMessage, json.RawMessage) error
	selectThread                              func(string, json.RawMessage) error
	done                                      chan struct{}
}

func newBrokerMux(ctx context.Context, native appTransport, observe func(string, json.RawMessage, json.RawMessage) error, selectThread ...func(string, json.RawMessage) error) *brokerMux {
	ctx, cancel := context.WithCancel(ctx)
	m := &brokerMux{ctx: ctx, cancel: cancel, native: native, byteLimit: 2 * brokerMessageLimit, calls: map[string]*brokerCall{}, tuiIDs: map[string]bool{}, servers: map[string]*brokerServerCall{}, nativeOut: make(chan brokerPacket, brokerRouteLimit), tuiOut: make(chan brokerPacket, brokerRouteLimit), ready: make(chan struct{}), observe: observe, done: make(chan struct{})}
	if len(selectThread) > 0 {
		m.selectThread = selectThread[0]
	}
	go func() { <-ctx.Done(); m.fail(ctx.Err()) }()
	go m.writeNative()
	go m.readNative()
	return m
}
func brokerID(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", errors.New("App Server ID must not be null")
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return "s:" + s, nil
	}
	var n int64
	if json.Unmarshal(raw, &n) == nil && string(raw) != "null" {
		return "n:" + strconv.FormatInt(n, 10), nil
	}
	return "", errors.New("App Server ID must be a string or integer")
}
func brokerRaw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func serverID(raw json.RawMessage) json.RawMessage {
	return brokerRaw("sessionbus/server/" + base64.RawURLEncoding.EncodeToString(raw))
}
func brokerMethod(f brokerFrame) (string, error) {
	if v, ok := f["method"]; ok {
		var method string
		if json.Unmarshal(v, &method) != nil || method == "" {
			return "", errors.New("invalid App Server method")
		}
		return method, nil
	}
	return "", nil
}
func brokerResponse(f brokerFrame) error {
	_, result := f["result"]
	_, failure := f["error"]
	if result == failure {
		return errors.New("App Server response needs exactly one result or error")
	}
	return nil
}
func (m *brokerMux) reserve(n int) error {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if n > m.byteLimit-m.buffered {
		return errors.New("App Server buffered byte budget exceeded")
	}
	m.buffered += n
	return nil
}
func (m *brokerMux) release(n int) { m.budgetMu.Lock(); m.buffered -= n; m.budgetMu.Unlock() }
func (m *brokerMux) enqueue(q chan brokerPacket, f brokerFrame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(b) > brokerMessageLimit {
		return errors.New("App Server frame exceeds native message limit")
	}
	if err = m.reserve(len(b)); err != nil {
		return err
	}
	select {
	case <-m.ctx.Done():
		m.release(len(b))
		return m.ctx.Err()
	default:
	}
	select {
	case q <- brokerPacket{body: b}:
		return nil
	default:
		m.release(len(b))
		return errors.New("App Server outbound queue is full")
	}
}

// errBrokerNotAdmitted marks a call that ended before its own request frame
// was admitted to the native outbound queue: that request never reached the
// native. It is call-local. A failure shared with other calls through fail is
// never tagged, so an admitted call cannot read as not admitted.
var errBrokerNotAdmitted = errors.New("App Server request was not admitted")

type brokerNotAdmitted struct{ cause error }

func (e *brokerNotAdmitted) Error() string        { return e.cause.Error() }
func (e *brokerNotAdmitted) Unwrap() error        { return e.cause }
func (e *brokerNotAdmitted) Is(target error) bool { return target == errBrokerNotAdmitted }

func (m *brokerMux) fail(err error) {
	var local *brokerNotAdmitted
	if errors.As(err, &local) {
		err = local.cause
	}
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return
	}
	m.err = err
	calls := m.calls
	m.calls = map[string]*brokerCall{}
	m.mu.Unlock()
	m.cancel()
	_ = m.native.Close()
	for _, c := range calls {
		if c.reply != nil {
			c.reply <- appReply{err: err}
		}
	}
}
func (m *brokerMux) writeNative() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case f := <-m.nativeOut:
			err := m.native.Write(f.body)
			m.release(len(f.body))
			if err != nil {
				m.fail(err)
				return
			}
		}
	}
}
func (m *brokerMux) readNative() {
	defer close(m.done)
	for m.ctx.Err() == nil {
		var f brokerFrame
		if err := m.native.Read(&f); err != nil {
			m.fail(err)
			return
		}
		if err := m.fromNative(f); err != nil {
			m.fail(err)
			return
		}
	}
}

// fromTUI runs on the sole WebSocket reader. No second initialize is generated.
func (m *brokerMux) fromTUI(f brokerFrame) error {
	method, err := brokerMethod(f)
	if err != nil {
		return err
	}
	id, hasID := f["id"]
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if method != "" {
		if hasID {
			key, err := brokerID(id)
			if err != nil {
				return err
			}
			if m.tuiIDs[key] {
				return errors.New("duplicate TUI request ID")
			}
			if len(m.calls) >= brokerRouteLimit {
				return errors.New("App Server request limit reached")
			}
			if method == "initialize" {
				if m.initializeSeen {
					return errors.New("duplicate TUI initialize")
				}
				m.initializeSeen = true
			}
			m.next++
			wire := fmt.Sprintf("sessionbus/client/%d", m.next)
			held := len(id) + len(key)
			if err := m.reserve(held); err != nil {
				return err
			}
			m.calls["s:"+wire] = &brokerCall{original: id, method: method, held: held}
			m.tuiIDs[key] = true
			f["id"] = brokerRaw(wire)
		} else if method == "initialized" {
			if !m.initializeOK || m.initialized {
				return errors.New("TUI initialized before successful initialize or twice")
			}
			m.initialized = true
		}
		if err := m.enqueue(m.nativeOut, f); err != nil {
			return err
		}
		if m.initialized {
			m.readyOnce.Do(func() { close(m.ready) })
		}
		return nil
	}
	if !hasID {
		return errors.New("TUI response has no ID")
	}
	if err := brokerResponse(f); err != nil {
		return err
	}
	key, err := brokerID(id)
	if err != nil {
		return err
	}
	c := m.servers[key]
	if c == nil {
		// Native resolution dismisses the TUI handler without a response. Its
		// deterministic mapped ID lets a racing late response drain without
		// retaining a tombstone or occupying a live request slot.
		var mapped string
		if json.Unmarshal(id, &mapped) == nil && strings.HasPrefix(mapped, "sessionbus/server/") {
			raw, decodeErr := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(mapped, "sessionbus/server/"))
			if _, idErr := brokerID(raw); decodeErr == nil && idErr == nil {
				return nil
			}
		}
		return errors.New("unknown TUI response ID")
	}
	delete(m.servers, key)
	m.release(c.held)
	f["id"] = c.original
	return m.enqueue(m.nativeOut, f)
}
func (m *brokerMux) fromNative(f brokerFrame) error {
	method, err := brokerMethod(f)
	if err != nil {
		return err
	}
	id, hasID := f["id"]
	if method != "" {
		m.mu.Lock()
		if hasID {
			if _, err := brokerID(id); err != nil {
				m.mu.Unlock()
				return err
			}
			mapped := serverID(id)
			key, _ := brokerID(mapped)
			if len(m.servers) >= brokerRouteLimit || m.servers[key] != nil {
				m.mu.Unlock()
				return errors.New("native request limit or duplicate ID")
			}
			held := len(id) + len(key)
			if err := m.reserve(held); err != nil {
				m.mu.Unlock()
				return err
			}
			m.servers[key] = &brokerServerCall{original: id, held: held}
			f["id"] = mapped
		} else if method == "serverRequest/resolved" {
			var p map[string]json.RawMessage
			if json.Unmarshal(f["params"], &p) != nil || p == nil {
				m.mu.Unlock()
				return errors.New("invalid resolved notification")
			}
			if _, err := brokerID(p["requestId"]); err != nil {
				m.mu.Unlock()
				return err
			}
			mapped := serverID(p["requestId"])
			key, _ := brokerID(mapped)
			if c := m.servers[key]; c != nil {
				delete(m.servers, key)
				m.release(c.held)
			}
			p["requestId"] = mapped
			f["params"] = brokerRaw(p)
		}
		m.mu.Unlock()
		if !hasID && m.observe != nil {
			if err := m.observe(method, f["params"], nil); err != nil {
				return err
			}
		}
		return m.enqueue(m.tuiOut, f)
	}
	if !hasID {
		return errors.New("native response has no ID")
	}
	if err := brokerResponse(f); err != nil {
		return err
	}
	key, err := brokerID(id)
	if err != nil {
		return err
	}
	m.mu.Lock()
	c := m.calls[key]
	if c == nil {
		m.mu.Unlock()
		return errors.New("unknown native response ID")
	}
	delete(m.calls, key)
	m.release(c.held)
	if c.original != nil {
		tuiKey, _ := brokerID(c.original)
		delete(m.tuiIDs, tuiKey)
	}
	if c.method == "initialize" {
		_, failed := f["error"]
		m.initializeOK = !failed
	}
	m.mu.Unlock()
	r := appReply{result: f["result"]}
	if raw, failed := f["error"]; failed {
		var e appError
		if json.Unmarshal(raw, &e) != nil {
			r.err = errors.New("invalid native error")
		} else {
			r.err = &e
		}
	}
	if r.err == nil && m.observe != nil && (c.original != nil || c.reply != nil) {
		r.err = m.observe(c.method, nil, f["result"])
	}
	if r.err == nil && c.original != nil && m.selectThread != nil {
		r.err = m.selectThread(c.method, f["result"])
	}
	if c.original == nil {
		if c.reply != nil {
			c.reply <- r
		}
		return nil
	}
	if r.err != nil {
		if _, nativeError := f["error"]; !nativeError {
			return r.err
		}
	}
	f["id"] = c.original
	return m.enqueue(m.tuiOut, f)
}

// call returns an error that matches errBrokerNotAdmitted when it ended before
// its request frame was queued to the native.
func (m *brokerMux) call(ctx context.Context, method string, params, result any) error {
	if err := ctx.Err(); err != nil {
		return &brokerNotAdmitted{err}
	}
	select {
	case <-ctx.Done():
		return &brokerNotAdmitted{ctx.Err()}
	case <-m.ctx.Done():
		return &brokerNotAdmitted{m.ctx.Err()}
	case <-m.ready:
	}
	p, err := json.Marshal(params)
	if err != nil {
		return &brokerNotAdmitted{err}
	}
	m.mu.Lock()
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return &brokerNotAdmitted{err}
	}
	if m.err != nil {
		err := m.err
		m.mu.Unlock()
		return &brokerNotAdmitted{err}
	}
	if len(m.calls) >= brokerRouteLimit {
		m.mu.Unlock()
		return &brokerNotAdmitted{errors.New("App Server request limit reached")}
	}
	m.next++
	id := fmt.Sprintf("sessionbus/client/%d", m.next)
	reply := make(chan appReply, 1)
	m.calls["s:"+id] = &brokerCall{method: method, reply: reply}
	err = m.enqueue(m.nativeOut, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": brokerRaw(id), "method": brokerRaw(method), "params": p})
	m.mu.Unlock()
	if err != nil {
		m.fail(err)
		return &brokerNotAdmitted{err}
	}
	var response appReply
	select {
	case response = <-reply:
	case <-ctx.Done():
		m.mu.Lock()
		pending := m.calls["s:"+id]
		if pending != nil {
			pending.reply = nil
		}
		m.mu.Unlock()
		if pending != nil {
			return ctx.Err()
		}
		// The native reader or failure path already owns completion. Its result
		// wins cancellation after admission; never submit a second native turn.
		response = <-reply
	}
	if response.err != nil {
		return response.err
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(response.result, result)
}
