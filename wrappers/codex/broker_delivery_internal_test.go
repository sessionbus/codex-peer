// SPDX-License-Identifier: MIT
package codex

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

func brokerDeliveryRequest(body string) kit.DeliveryRequest {
	return kit.DeliveryRequest{MessageID: "m" + body, From: kit.DeliverySource{SessionID: "source@local", Product: "fixture", Groups: []string{"launch"}}, Body: body}
}
func residentOf(o *brokerOwners) *brokerResident {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.owners["one"]
}
func eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !done(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
func held(o *brokerOwners) int {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()
	return o.deliveryHeld
}

func TestBrokerCallNotAdmittedIsCallLocal(t *testing.T) {
	m, native := muxFixture(t, nil)
	initializeMux(t, m, native)
	// Before admission: a cancelled caller and the route limit. The cause and its text are kept.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.call(cancelled, "turn/steer", nil, nil); !errors.Is(err, errBrokerNotAdmitted) || !errors.Is(err, context.Canceled) || err.Error() != context.Canceled.Error() {
		t.Fatalf("cancelled before admission: %v", err)
	}
	m.mu.Lock()
	for i := 0; i < brokerRouteLimit; i++ {
		m.calls["filler"+string(rune('a'+i%26))+strings.Repeat("x", i/26)] = &brokerCall{}
	}
	m.mu.Unlock()
	if err := m.call(context.Background(), "turn/steer", nil, nil); !errors.Is(err, errBrokerNotAdmitted) || err.Error() != "App Server request limit reached" {
		t.Fatalf("route limit: %v", err)
	}
	m.mu.Lock()
	m.calls = map[string]*brokerCall{}
	m.mu.Unlock()
	// After admission: a failure shared through fail must not read as not admitted, even when fail
	// is handed another call's not-admitted error.
	admitted := make(chan error, 1)
	go func() {
		admitted <- m.call(context.Background(), "turn/steer", map[string]string{"threadId": "one"}, nil)
	}()
	expectNative(t, native, "turn/steer")
	shared := errors.New("native connection lost")
	m.fail(&brokerNotAdmitted{shared})
	if err := <-admitted; errors.Is(err, errBrokerNotAdmitted) || !errors.Is(err, shared) {
		t.Fatalf("an admitted call read as not admitted: %v", err)
	}
}

func TestBrokerDeliveryOutcomeAfterATextCall(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		steer bool
		want  deliveryOutcome
	}{
		{"steer not admitted", &brokerNotAdmitted{errors.New("limit")}, true, deliveryKept},
		{"start not admitted", &brokerNotAdmitted{errors.New("limit")}, false, deliveryKept},
		{"steer refused -32600", &appError{Code: -32600, Message: "any wording"}, true, deliveryKept},
		{"steer internal -32603", &appError{Code: -32603, Message: "failed to steer turn"}, true, deliveryNativeError},
		{"steer too large -32602", &appError{Code: -32602, Message: "too long"}, true, deliveryNativeError},
		{"start refused -32600", &appError{Code: -32600, Message: "thread not found"}, false, deliveryNativeError},
		{"start non-submission -32603", &appError{Code: -32603, Message: "failed to submit turn input"}, false, deliveryNativeError},
		{"transport", io.EOF, true, deliveryLost},
		{"cancelled after admission", context.Canceled, false, deliveryLost},
	} {
		if got := afterTextCall(tc.err, tc.steer); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A request that fails before it is queued to the native was never attempted:
// the message is kept and delivered exactly once when the limit has cleared.
func TestBrokerDeliveryKeptWhenTheNativeRequestWasNotAdmitted(t *testing.T) {
	_, m, native, bus := deliveryFixture(t)
	m.mu.Lock()
	for i := 0; i < brokerRouteLimit; i++ {
		m.calls["filler"+string(rune('a'+i%26))+strings.Repeat("x", i/26)] = &brokerCall{}
	}
	m.mu.Unlock()
	sendDelivery(t, bus, 2, "LIMIT-BODY")
	expectWritten(t, bus, 2)
	m.mu.Lock()
	m.calls = map[string]*brokerCall{}
	m.mu.Unlock()
	nativeEvent(t, native, m, "turn/started", map[string]any{"threadId": "one", "turn": map[string]string{"id": "turn-1"}})
	steer := expectSteer(t, native, "turn-1")
	if text := inputText(t, steer); !strings.Contains(text, "LIMIT-BODY") {
		t.Fatalf("kept input: %q", text)
	}
	answerNative(t, native, steer, map[string]string{"turnId": "turn-1"})
	sendDelivery(t, bus, 3, "NEXT-BODY")
	expectWritten(t, bus, 3)
	if text := expectStart(t, native, "turn-2"); !strings.Contains(text, "NEXT-BODY") || strings.Contains(text, "LIMIT-BODY") {
		t.Fatalf("after the kept message: %q", text)
	}
}

// The native requests on the accepted paths are what they were before the queue.
func TestBrokerDeliveryNativeRequestsAreUnchanged(t *testing.T) {
	o, _, native, _ := deliveryFixture(t)
	r := residentOf(o)
	first, second := brokerDeliveryRequest("FIRST-BODY"), brokerDeliveryRequest("SECOND-BODY")
	if receipt, err := r.accept(first); err != nil || receipt != (kit.DeliveryReceipt{Disposition: "written"}) {
		t.Fatal(receipt, err)
	}
	answerThread(t, native, "idle")
	start := expectNative(t, native, "turn/start")
	rendered, _ := host.RenderNativeMessage(first)
	if want := brokerRaw(map[string]any{"threadId": "one", "input": textInput(rendered)}); string(start["params"]) != string(want) {
		t.Fatalf("turn/start params %s, want %s", start["params"], want)
	}
	answerNative(t, native, start, map[string]any{"turn": map[string]string{"id": "turn-1"}})
	if _, err := r.accept(second); err != nil {
		t.Fatal(err)
	}
	read := expectNative(t, native, "thread/read")
	if want := brokerRaw(map[string]any{"threadId": "one", "includeTurns": false}); string(read["params"]) != string(want) {
		t.Fatalf("thread/read params %s", read["params"])
	}
	answerNative(t, native, read, map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "active"}}})
	turns := expectNative(t, native, "thread/turns/list")
	if want := brokerRaw(map[string]any{"threadId": "one", "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"}); string(turns["params"]) != string(want) {
		t.Fatalf("thread/turns/list params %s", turns["params"])
	}
	answerNative(t, native, turns, map[string]any{"data": []any{map[string]string{"id": "turn-1", "status": "inProgress"}}})
	steer := expectNative(t, native, "turn/steer")
	rendered, _ = host.RenderNativeMessage(second)
	if want := brokerRaw(map[string]any{"threadId": "one", "expectedTurnId": "turn-1", "input": textInput(rendered)}); string(steer["params"]) != string(want) {
		t.Fatalf("turn/steer params %s, want %s", steer["params"], want)
	}
	answerNative(t, native, steer, map[string]string{"turnId": "turn-1"})
	eventually(t, "the queue to drain", func() bool { return held(o) == 0 })
}

func TestBrokerDeliveryBoundsUseTheExistingRefusal(t *testing.T) {
	full := kit.DeliveryReceipt{Disposition: "rejected", Reason: "Sessionbus unsent input limit reached"}
	written := kit.DeliveryReceipt{Disposition: "written"}
	t.Run("64 messages, the head in flight still counted", func(t *testing.T) {
		o, _, native, _ := deliveryFixture(t)
		r := residentOf(o)
		for i := 0; i < brokerDeliveryMessages; i++ {
			if receipt, err := r.accept(brokerDeliveryRequest("x")); err != nil || receipt != written {
				t.Fatal(i, receipt, err)
			}
		}
		expectNative(t, native, "thread/read") // the head is being attempted and is still counted
		if receipt, err := r.accept(brokerDeliveryRequest("x")); err != nil || receipt != full {
			t.Fatal(receipt, err)
		}
	})
	t.Run("1 MiB per thread, counted on the rendered message", func(t *testing.T) {
		o, _, _, _ := deliveryFixture(t)
		r := residentOf(o)
		large, more := brokerDeliveryRequest(strings.Repeat("a", 400_000)), brokerDeliveryRequest(strings.Repeat("b", 200_000))
		first, _ := host.RenderNativeMessage(large)
		second, _ := host.RenderNativeMessage(more)
		if len(first) > brokerDeliveryBytes || len(first)+len(second) <= brokerDeliveryBytes {
			t.Fatalf("fixture sizes %d and %d do not straddle the bound", len(first), len(second))
		}
		if receipt, err := r.accept(large); err != nil || receipt != written {
			t.Fatal(receipt, err)
		}
		if receipt, err := r.accept(more); err != nil || receipt != full {
			t.Fatal(receipt, err)
		}
		if held(o) != len(first) {
			t.Fatalf("held %d bytes, want the one accepted message's %d", held(o), len(first))
		}
	})
	t.Run("16 MiB per broker, released once", func(t *testing.T) {
		o, m, native, _ := deliveryFixture(t)
		r := residentOf(o)
		o.deliveryMu.Lock()
		o.deliveryHeld = brokerDeliveryTotal - 16
		o.deliveryMu.Unlock()
		if receipt, err := r.accept(brokerDeliveryRequest("too much for the broker")); err != nil || receipt != full {
			t.Fatal(receipt, err)
		}
		o.deliveryMu.Lock()
		o.deliveryHeld = 0
		o.deliveryMu.Unlock()
		if receipt, err := r.accept(brokerDeliveryRequest("fits")); err != nil || receipt != written {
			t.Fatal(receipt, err)
		}
		if held(o) == 0 {
			t.Fatal("an accepted message holds no bytes")
		}
		// The thread closing discards the queue and releases its bytes exactly once.
		nativeEvent(t, native, m, "thread/closed", map[string]string{"threadId": "one"})
		eventually(t, "the discarded queue to release its bytes", func() bool { return held(o) == 0 })
		r.mu.Lock()
		left := len(r.queue)
		r.mu.Unlock()
		if left != 0 {
			t.Fatal("queue kept after the thread closed", left)
		}
		if receipt, err := r.accept(brokerDeliveryRequest("late")); err != nil || receipt != (kit.DeliveryReceipt{Disposition: "rejected", Reason: "closing"}) {
			t.Fatal(receipt, err)
		}
		if held(o) != 0 {
			t.Fatal("a refused message holds bytes")
		}
	})
}

// The queue belongs to the published thread, not to one bus connection.
func TestBrokerDeliverySurvivesTheReceiptConnection(t *testing.T) {
	o, _, native, bus := deliveryFixture(t)
	sendDelivery(t, bus, 2, "SURVIVE-BODY")
	expectWritten(t, bus, 2)
	read := expectNative(t, native, "thread/read") // the attempt is in flight
	_ = bus.Close()
	answerNative(t, native, read, map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "idle"}}})
	start := expectNative(t, native, "turn/start")
	if text := inputText(t, start); !strings.Contains(text, "SURVIVE-BODY") {
		t.Fatalf("input after the bus connection closed: %q", text)
	}
	answerNative(t, native, start, map[string]any{"turn": map[string]string{"id": "turn-1"}})
	eventually(t, "the queue to drain", func() bool { return held(o) == 0 })
}

// An event that arrives while an attempt is in flight is not lost: the kept
// message is tried again without any further event.
func TestBrokerDeliveryWakeDuringAnAttemptIsNotLost(t *testing.T) {
	o, m, native, bus := deliveryFixture(t)
	r := residentOf(o)
	sendDelivery(t, bus, 2, "WAKE-BODY")
	expectWritten(t, bus, 2)
	read := expectNative(t, native, "thread/read")
	nativeEvent(t, native, m, "turn/completed", map[string]any{"threadId": "one", "turn": map[string]string{"id": "turn-0", "status": "completed"}})
	if len(r.changed) != 0 {
		t.Fatal("the delivery wake used the publisher's channel")
	}
	answerNative(t, native, read, map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "notLoaded"}}}) // kept: nothing was sent
	if text := expectStart(t, native, "turn-1"); !strings.Contains(text, "WAKE-BODY") {
		t.Fatalf("input after the pending wake: %q", text)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *lockedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}
func (w *lockedBuffer) String() string { w.mu.Lock(); defer w.mu.Unlock(); return w.b.String() }

func TestBrokerDeliveryDropDiagnosticCarriesNoContent(t *testing.T) {
	out := &lockedBuffer{}
	previous := brokerDiagnostics
	brokerDiagnostics = out
	t.Cleanup(func() { brokerDiagnostics = previous })
	o, _, native, bus := deliveryFixture(t)
	sendDelivery(t, bus, 2, "SECRET-MESSAGE-BODY")
	expectWritten(t, bus, 2)
	refuseNative(t, native, expectSteer(t, native, "turn-1"), -32603, "SECRET-NATIVE-ERROR-TEXT")
	eventually(t, "the uncertain message to be dropped", func() bool { return held(o) == 0 })
	if early := out.String(); early != "" {
		t.Fatalf("written while the TUI may still own the terminal: %q", early)
	}
	o.End()
	o.End() // reported once
	if line := out.String(); line != "sessionbus: Codex interactive delivery not confirmed and not replayed (turn/steer, native error): 1\n" {
		t.Fatalf("diagnostic %q", line)
	}
}
