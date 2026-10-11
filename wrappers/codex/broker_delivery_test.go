// SPDX-License-Identifier: MIT
package codex

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

// These tests drive the interactive delivery through the bus connection and
// the native pipe only, so they also run against the previous implementation,
// where the receipt waited for the native and a refused steer was lost.

func deliveryFixture(t *testing.T) (*brokerOwners, *brokerMux, *muxPipe, *muxPipe) {
	t.Helper()
	o, m, native, buses := residentFixture(t)
	loaded(t, o, m, native, "one")
	readyCatalog(t, m, native, "one")
	bus := receiveBus(t, buses)
	acceptHello(t, bus)
	return o, m, native, bus
}
func sendDelivery(t *testing.T, bus *muxPipe, id int, body string) {
	t.Helper()
	request := kit.DeliveryRequest{MessageID: "m" + body, From: kit.DeliverySource{SessionID: "source@local", Product: "fixture", Groups: []string{"launch"}}, Body: body}
	if err := bus.Write(frame("message.deliver", id, request)); err != nil {
		t.Fatal(err)
	}
}

// expectWritten requires the bus receipt while no native request has been
// answered: the receipt is the message reaching this product.
func expectWritten(t *testing.T, bus *muxPipe, id int) {
	t.Helper()
	_ = bus.SetReadDeadline(time.Now().Add(2 * time.Second))
	var receipt brokerFrame
	if err := bus.Read(&receipt); err != nil {
		t.Fatalf("no receipt arrived while the native had not answered: %v", err)
	}
	if string(receipt["id"]) != string(brokerRaw(id)) || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatalf("receipt before native admission: id=%s result=%s error=%s", receipt["id"], receipt["result"], receipt["error"])
	}
}
func expectNative(t *testing.T, native *muxPipe, method string) brokerFrame {
	t.Helper()
	request := receiveNative(t, native)
	if string(request["method"]) != string(brokerRaw(method)) {
		t.Fatalf("native request %s, want %s", request["method"], method)
	}
	return request
}
func answerNative(t *testing.T, native *muxPipe, request brokerFrame, result any) {
	t.Helper()
	if err := native.Write(brokerFrame{"id": request["id"], "result": brokerRaw(result)}); err != nil {
		t.Fatal(err)
	}
}
func refuseNative(t *testing.T, native *muxPipe, request brokerFrame, code int, message string) {
	t.Helper()
	if err := native.Write(brokerFrame{"id": request["id"], "error": brokerRaw(map[string]any{"code": code, "message": message})}); err != nil {
		t.Fatal(err)
	}
}
func answerThread(t *testing.T, native *muxPipe, status string) {
	t.Helper()
	answerNative(t, native, expectNative(t, native, "thread/read"), map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": status}}})
}
func answerTurns(t *testing.T, native *muxPipe, turn, status string) {
	t.Helper()
	answerNative(t, native, expectNative(t, native, "thread/turns/list"), map[string]any{"data": []any{map[string]string{"id": turn, "status": status}}})
}
func inputText(t *testing.T, request brokerFrame) string {
	t.Helper()
	var params struct {
		ThreadID string
		Input    []struct{ Type, Text string }
	}
	if json.Unmarshal(request["params"], &params) != nil || params.ThreadID != "one" || len(params.Input) != 1 || params.Input[0].Type != "text" {
		t.Fatalf("native input: %s", request["params"])
	}
	return params.Input[0].Text
}

// expectSteer answers the read and the turn list for an active turn and
// returns the steer request.
func expectSteer(t *testing.T, native *muxPipe, turn string) brokerFrame {
	t.Helper()
	answerThread(t, native, "active")
	answerTurns(t, native, turn, "inProgress")
	steer := expectNative(t, native, "turn/steer")
	var params struct{ ExpectedTurnID string }
	if json.Unmarshal(steer["params"], &params) != nil || params.ExpectedTurnID != turn {
		t.Fatalf("steer: %s", steer["params"])
	}
	return steer
}
func expectStart(t *testing.T, native *muxPipe, turn string) string {
	t.Helper()
	answerThread(t, native, "idle")
	start := expectNative(t, native, "turn/start")
	text := inputText(t, start)
	answerNative(t, native, start, map[string]any{"turn": map[string]string{"id": turn}})
	return text
}

func TestBrokerDeliveryReceiptPrecedesNativeAdmission(t *testing.T) {
	_, _, native, bus := deliveryFixture(t)
	sendDelivery(t, bus, 2, "IDLE-BODY")
	expectWritten(t, bus, 2) // the native has answered nothing yet
	if text := expectStart(t, native, "turn-1"); !strings.Contains(text, "IDLE-BODY") {
		t.Fatalf("idle wake input: %q", text)
	}
}

func TestBrokerDeliveryKeepsARefusedSteerForTheNextTurn(t *testing.T) {
	for _, tc := range []struct{ name, refusal, ending string }{
		{"no active turn", "no active turn to steer", "completed"},
		{"compact turn", "cannot steer a compact turn", "completed"},
		{"a wording this build has never seen", "steering is unavailable right now", "completed"},
		{"the human interrupted the turn", "no active turn to steer", "interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, m, native, bus := deliveryFixture(t)
			sendDelivery(t, bus, 2, "FIRST-BODY")
			expectWritten(t, bus, 2)
			steer := expectSteer(t, native, "turn-1")
			kept := inputText(t, steer)
			refuseNative(t, native, steer, -32600, tc.refusal) // the native took nothing
			nativeEvent(t, native, m, "turn/completed", map[string]any{"threadId": "one", "turn": map[string]string{"id": "turn-1", "status": tc.ending}})
			if text := expectStart(t, native, "turn-2"); text != kept {
				t.Fatalf("the kept message did not start the next turn unchanged: %q, want %q", text, kept)
			}
			// It was delivered exactly once: the next input is the next message.
			sendDelivery(t, bus, 3, "SECOND-BODY")
			expectWritten(t, bus, 3)
			if text := expectStart(t, native, "turn-3"); !strings.Contains(text, "SECOND-BODY") || strings.Contains(text, "FIRST-BODY") {
				t.Fatalf("after the kept message: %q", text)
			}
		})
	}
}

func TestBrokerDeliveryKeepsInputWhenTheActiveTurnHasEnded(t *testing.T) {
	_, m, native, bus := deliveryFixture(t)
	sendDelivery(t, bus, 2, "BOUNDARY-BODY")
	expectWritten(t, bus, 2)
	answerThread(t, native, "active")
	answerTurns(t, native, "turn-1", "completed") // active, but no turn in progress: nothing is sent
	nativeEvent(t, native, m, "thread/status/changed", map[string]any{"threadId": "one", "status": map[string]string{"type": "idle"}})
	if text := expectStart(t, native, "turn-2"); !strings.Contains(text, "BOUNDARY-BODY") {
		t.Fatalf("kept input: %q", text)
	}
}

func TestBrokerDeliveryNeverReplaysAnUncertainSteer(t *testing.T) {
	for _, code := range []int{-32603, -32602} {
		_, m, native, bus := deliveryFixture(t)
		sendDelivery(t, bus, 2, "FIRST-BODY")
		expectWritten(t, bus, 2)
		refuseNative(t, native, expectSteer(t, native, "turn-1"), code, "failed to steer turn")
		nativeEvent(t, native, m, "turn/completed", map[string]any{"threadId": "one", "turn": map[string]string{"id": "turn-1", "status": "completed"}})
		// Nothing carries the first message again; the next input is the next message.
		sendDelivery(t, bus, 3, "SECOND-BODY")
		expectWritten(t, bus, 3)
		if text := expectStart(t, native, "turn-2"); !strings.Contains(text, "SECOND-BODY") || strings.Contains(text, "FIRST-BODY") {
			t.Fatalf("code %d: after an uncertain steer: %q", code, text)
		}
	}
}

func TestBrokerDeliveriesKeepTheirOrder(t *testing.T) {
	_, _, native, bus := deliveryFixture(t)
	sendDelivery(t, bus, 2, "FIRST-BODY")
	expectWritten(t, bus, 2)
	sendDelivery(t, bus, 3, "SECOND-BODY")
	expectWritten(t, bus, 3) // accepted while the first is still waiting for the native
	if text := expectStart(t, native, "turn-1"); !strings.Contains(text, "FIRST-BODY") {
		t.Fatalf("first input: %q", text)
	}
	steer := expectSteer(t, native, "turn-1")
	if text := inputText(t, steer); !strings.Contains(text, "SECOND-BODY") || strings.Contains(text, "FIRST-BODY") {
		t.Fatalf("second input: %q", text)
	}
	answerNative(t, native, steer, map[string]string{"turnId": "turn-1"})
}

// This test does not depend on when the receipt arrives, so on the previous
// implementation it reaches the refusal and shows the loss itself: the refused
// message is answered as an error and never offered to the native again.
func TestBrokerDeliveryRefusedSteerIsNotLost(t *testing.T) {
	_, m, native, bus := deliveryFixture(t)
	answers := make(chan brokerFrame, 1)
	go func() {
		var answer brokerFrame
		if bus.Read(&answer) == nil {
			answers <- answer
		}
	}()
	sendDelivery(t, bus, 2, "KEEP-BODY")
	steer := expectSteer(t, native, "turn-1")
	kept := inputText(t, steer)
	refuseNative(t, native, steer, -32600, "no active turn to steer")
	var answer brokerFrame
	select {
	case answer = <-answers:
	case <-time.After(3 * time.Second):
		t.Fatal("the bus got no answer")
	}
	nativeEvent(t, native, m, "turn/completed", map[string]any{"threadId": "one", "turn": map[string]string{"id": "turn-1", "status": "completed"}})
	_ = native.SetReadDeadline(time.Now().Add(2 * time.Second))
	var read brokerFrame
	if err := native.Read(&read); err != nil {
		t.Fatalf("the refused message was never offered to the native again (bus answer: result=%s error=%s)", answer["result"], answer["error"])
	}
	if string(read["method"]) != `"thread/read"` {
		t.Fatal(read)
	}
	answerNative(t, native, read, map[string]any{"thread": map[string]any{"id": "one", "status": map[string]string{"type": "idle"}}})
	start := expectNative(t, native, "turn/start")
	if text := inputText(t, start); text != kept {
		t.Fatalf("next turn input %q, want the kept message %q", text, kept)
	}
	answerNative(t, native, start, map[string]any{"turn": map[string]string{"id": "turn-2"}})
	if string(answer["result"]) != `{"disposition":"written"}` {
		t.Fatalf("bus answer: result=%s error=%s", answer["result"], answer["error"])
	}
}
