// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// A transfer refusal reaches the caller as its own code and leaves the
// thread's daemon connection serving later calls. The U6 offer to a peer
// without transfers got -32008 with data from core a831, which no client could
// decode; the fixed daemon answers unsupported_transfer or transfer_failed
// without data, which a decoder older than that fix also rejects.
func TestBrokerTransferRefusalKeepsTheConnection(t *testing.T) {
	cwd := t.TempDir()
	o, _, _, bus, _, _ := transferBroker(t, cwd, nil, false)
	defer o.End()
	for _, refusal := range []struct {
		action, arguments, method, message string
		code                               int
	}{
		{"transfer.offer", `{"target":"peer@local","source":"missing-is-fine.txt"}`, "transfer.offer", "unsupported_transfer", protocol.UnsupportedTransfer},
		{"transfer.status", `{"id":"tr-unknown"}`, "transfer.status", "transfer_failed", protocol.TransferFailed},
	} {
		type actionResult struct {
			out json.RawMessage
			err error
		}
		done := make(chan actionResult, 1)
		go func() {
			out, err := o.action(context.Background(), refusal.action, json.RawMessage(refusal.arguments), json.RawMessage(`{"threadId":"one"}`))
			done <- actionResult{out, err}
		}()
		request := receiveNative(t, bus)
		if string(request["method"]) != `"`+refusal.method+`"` {
			t.Fatalf("wire %s = %s", refusal.action, request["method"])
		}
		writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": request["id"], "error": brokerRaw(map[string]any{"code": refusal.code, "message": refusal.message})})
		got := <-done
		var failure *kit.ProtocolError
		if !errors.As(got.err, &failure) || failure.Code != refusal.code || failure.Message != refusal.message {
			t.Fatalf("%s refusal = %s, %v; want %d %s", refusal.action, got.out, got.err, refusal.code, refusal.message)
		}
		listed := make(chan error, 1)
		go func() {
			_, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`))
			listed <- err
		}()
		list := receiveNative(t, bus)
		if string(list["method"]) != `"session.list"` {
			t.Fatalf("after the %s refusal = %s", refusal.action, list["method"])
		}
		writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": list["id"], "result": brokerRaw(map[string]any{"sessions": []any{}})})
		if err := <-listed; err != nil {
			t.Fatalf("the connection did not serve a call after the %s refusal: %v", refusal.action, err)
		}
	}
}
