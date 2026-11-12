// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/mcp"
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
		args := json.RawMessage(tc.args)
		got, err := mcp.BindTransferPaths(tc.action, args, tc.cwd)
		if string(args) != tc.args {
			t.Fatalf("%s: caller arguments changed: %s", tc.name, args)
		}
		if err != nil || string(got) != tc.want {
			t.Fatalf("%s: got %s, %v; want %s", tc.name, got, err, tc.want)
		}
	}
}

// Without the thread's working directory a relative path is refused rather
// than resolved against this process's directory.
func TestBindTransferPathsRefusesARelativePathWithoutTheThreadCwd(t *testing.T) {
	for _, cwd := range []string{"", "relative/dir"} {
		if _, err := mcp.BindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`), cwd); err == nil ||
			!strings.Contains(err.Error(), "absolute path") {
			t.Fatalf("cwd %q: %v", cwd, err)
		}
	}
	if got, err := mcp.BindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"/a.txt"}`), ""); err != nil || string(got) != `{"target":"t","source":"/a.txt"}` {
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
	o, _, _, bus, _, hello := transferBroker(t, cwd, nil, false)
	var helloParams struct{ Info map[string]any }
	if json.Unmarshal(hello["params"], &helloParams) != nil ||
		helloParams.Info[protocol.TransferInfoKey] != float64(1) || helloParams.Info["cwd"] != cwd {
		t.Fatalf("hello = %s", hello["params"])
	}
	offerAndPrepare(t, o, bus, "tr-one", len(content))
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
