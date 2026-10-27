// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestBindTransferPathsJoinsOnlyRelativeLocalPaths(t *testing.T) {
	for _, tc := range []struct{ name, action, args, cwd, want string }{
		{"relative source", "transfer.offer", `{"target":"t","source":"docs/a.txt"}`, "/work", `{"source":"/work/docs/a.txt","target":"t"}`},
		{"absolute source kept", "transfer.offer", `{"target":"t","source":"/etc/a.txt"}`, "/work", `{"target":"t","source":"/etc/a.txt"}`},
		{"relative destination", "transfer.request", `{"target":"t","what":"w","destination":"out.bin","overwrite":true}`, "/work/", `{"destination":"/work/out.bin","overwrite":true,"target":"t","what":"w"}`},
		{"dot-dot kept for the filesystem", "transfer.accept", `{"id":"x","destination":"../up.txt"}`, "/work/sub", `{"destination":"/work/sub/../up.txt","id":"x"}`},
		{"accept source", "transfer.accept", `{"id":"x","source":"a.txt"}`, "/w", `{"id":"x","source":"/w/a.txt"}`},
		{"root cwd", "transfer.offer", `{"target":"t","source":"a"}`, "/", `{"source":"/a","target":"t"}`},
		{"other action untouched", "send", `{"target":"t","message":"source"}`, "", `{"target":"t","message":"source"}`},
		{"status untouched", "transfer.status", `{"id":"x"}`, "", `{"id":"x"}`},
		{"empty path left to the SDK", "transfer.offer", `{"target":"t","source":""}`, "", `{"target":"t","source":""}`},
		{"non-string left to the SDK", "transfer.offer", `{"target":"t","source":7}`, "", `{"target":"t","source":7}`},
		{"invalid JSON left to the SDK", "transfer.offer", `{`, "", `{`},
	} {
		got, err := bindTransferPaths(tc.action, json.RawMessage(tc.args), tc.cwd)
		if err != nil || string(got) != tc.want {
			t.Fatalf("%s: got %s, %v; want %s", tc.name, got, err, tc.want)
		}
	}
}

// Without the thread's working directory a relative path is refused rather
// than resolved against this process's directory.
func TestBindTransferPathsRefusesARelativePathWithoutTheThreadCwd(t *testing.T) {
	for _, cwd := range []string{"", "relative/dir"} {
		if _, err := bindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`), cwd); err == nil ||
			!strings.Contains(err.Error(), "absolute path") {
			t.Fatalf("cwd %q: %v", cwd, err)
		}
	}
	if got, err := bindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"/a.txt"}`), ""); err != nil || string(got) != `{"target":"t","source":"/a.txt"}` {
		t.Fatalf("absolute path without cwd = %s, %v", got, err)
	}
}

// An interactive thread's transfer offer is negotiated in the hello, sends
// no path on the wire, and is read from the thread's own working directory
// when the daemon asks the source to prepare.
func TestBrokerTransferOfferUsesTheThreadCwdAndTheSDKServesItsPrepare(t *testing.T) {
	cwd := t.TempDir()
	content := "transfer body\n"
	if err := os.WriteFile(filepath.Join(cwd, "notes.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	o, m, p, buses := residentFixture(t)
	nativeEvent(t, p, m, "thread/started", map[string]any{"thread": map[string]any{"id": "one", "cwd": cwd, "status": map[string]string{"type": "idle"}}})
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	hello := receiveNative(t, bus)
	var helloParams struct{ Info map[string]any }
	if string(hello["method"]) != `"session.hello"` || json.Unmarshal(hello["params"], &helloParams) != nil ||
		helloParams.Info[protocol.TransferInfoKey] != float64(1) || helloParams.Info["cwd"] != cwd {
		t.Fatalf("hello = %s", hello["params"])
	}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": hello["id"], "result": brokerRaw(map[string]any{"capabilities": []string{protocol.TransferCapability}})}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		o.mu.Lock()
		r := o.owners["one"]
		o.mu.Unlock()
		r.mu.Lock()
		admitted := r.admitted
		r.mu.Unlock()
		if admitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("thread was not admitted")
		}
		time.Sleep(time.Millisecond)
	}

	type actionResult struct {
		out json.RawMessage
		err error
	}
	// The SDK attaches transfers in the hello observer after our admission
	// callback; a response read after the hello frame proves both ran.
	listed := make(chan actionResult, 1)
	go func() {
		out, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`))
		listed <- actionResult{out, err}
	}()
	list := receiveNative(t, bus)
	if string(list["method"]) != `"session.list"` {
		t.Fatalf("barrier = %s", list["method"])
	}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": list["id"], "result": brokerRaw(map[string]any{"sessions": []any{}})}); err != nil {
		t.Fatal(err)
	}
	if got := <-listed; got.err != nil {
		t.Fatal(got.err)
	}
	offered := make(chan actionResult, 1)
	go func() {
		out, err := o.action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"peer@local","source":"notes.txt"}`), json.RawMessage(`{"threadId":"one"}`))
		offered <- actionResult{out, err}
	}()
	offer := receiveNative(t, bus)
	if string(offer["method"]) != `"transfer.offer"` || string(offer["params"]) != `{"target":"peer@local","name":"notes.txt"}` {
		t.Fatalf("wire offer = %s %s", offer["method"], offer["params"])
	}
	descriptor := map[string]any{"id": "tr-one", "sender": "one", "receiver": "peer@local", "name": "notes.txt", "created_by": "one", "state": "offered", "bytes": 0}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": offer["id"], "result": brokerRaw(descriptor)}); err != nil {
		t.Fatal(err)
	}
	if got := <-offered; got.err != nil || !strings.Contains(string(got.out), `"id":"tr-one"`) {
		t.Fatalf("offer result = %s, %v", got.out, got.err)
	}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": brokerRaw(900), "method": brokerRaw("transfer.prepare"), "params": brokerRaw(map[string]any{"id": "tr-one", "role": "source"})}); err != nil {
		t.Fatal(err)
	}
	prepared := receiveNative(t, bus)
	var result struct {
		ID   string
		Size int
	}
	if string(prepared["id"]) != "900" || json.Unmarshal(prepared["result"], &result) != nil || result.ID != "tr-one" || result.Size != len(content) {
		t.Fatalf("prepare reply = %s", prepared["result"])
	}
	_ = bus.Close()
	o.End()
}

// A lane binds transfer paths to the thread's verified working directory
// before the SDK sees them: without it a relative path is refused here, and
// with it the action reaches the SDK (here a caller without transfers).
func TestLaneToolBindsTransferPathsToTheThreadCwd(t *testing.T) {
	p := New()
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	owner := &laneToolOwner{owner: p}
	if _, err := owner.Action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`)); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("relative path before open = %v", err)
	}
	p.mu.Lock()
	p.cwd = "/lane/work"
	p.mu.Unlock()
	if _, err := owner.Action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`)); err == nil || strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("relative path after open = %v", err)
	}
}
