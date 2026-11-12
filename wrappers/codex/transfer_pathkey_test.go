// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/mcp"
)

// recordingTransferExecutor records the path the SDK opens at prepare and
// opens nothing unless real is set.
type recordingTransferExecutor struct {
	mu    sync.Mutex
	paths []string
	real  bool
}

func (e *recordingTransferExecutor) record(path string) {
	e.mu.Lock()
	e.paths = append(e.paths, path)
	e.mu.Unlock()
}

func (e *recordingTransferExecutor) recorded() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.paths...)
}

func (e *recordingTransferExecutor) OpenTransferSource(ctx context.Context, path string) (kit.TransferSource, error) {
	e.record(path)
	if e.real {
		return kit.FilesystemTransferExecutor{}.OpenTransferSource(ctx, path)
	}
	return nil, errors.New("recorded, not opened")
}

func (e *recordingTransferExecutor) OpenTransferDestination(ctx context.Context, path string, overwrite bool) (kit.TransferDestination, error) {
	e.record(path)
	if e.real {
		return kit.FilesystemTransferExecutor{}.OpenTransferDestination(ctx, path, overwrite)
	}
	return nil, errors.New("recorded, not opened")
}

// The SDK matches a transfer field case-insensitively (Go's decoder, which
// also folds "ſ" to "s"), so another spelling of a path key would reach it
// unbound. The binder refuses such a key, as the SDK refuses a field it does
// not know, for every path-carrying action, also mixed with the canonical key.
func TestBindTransferPathsRefusesAnotherSpellingOfAPathKey(t *testing.T) {
	for _, test := range []struct{ action, args string }{
		{"transfer.offer", `{"target":"t","Source":"a.txt"}`},
		{"transfer.offer", `{"target":"t","ſource":"a.txt"}`},
		{"transfer.offer", `{"target":"t","source":"/abs/a.txt","SOURCE":"a.txt"}`},
		{"transfer.request", `{"target":"t","what":"w","Destination":"a.txt","overwrite":true}`},
		{"transfer.accept", `{"id":"x","Source":"a.txt"}`},
		{"transfer.accept", `{"id":"x","destinatioN":"a.txt"}`},
	} {
		for _, cwd := range []string{"/thread/work", ""} {
			if out, err := mcp.BindTransferPaths(test.action, json.RawMessage(test.args), cwd); err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("%s %s (cwd %q) = %s, %v", test.action, test.args, cwd, out, err)
			}
		}
	}
	// The documented spelling is bound as before.
	out, err := mcp.BindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`), "/thread/work")
	if err != nil || string(out) != `{"source":"/thread/work/a.txt","target":"t"}` {
		t.Fatalf("canonical offer = %s, %v", out, err)
	}
}

// pathKeyRefused sends one action through the real broker and requires it to
// be refused before any frame reaches the daemon: the next frame the daemon
// sees must be the canonical offer sent after it.
func pathKeyRefused(t *testing.T, o *brokerOwners, bus *muxPipe, action, args string) {
	t.Helper()
	frames := make(chan brokerFrame, 1)
	go func() {
		_ = bus.SetReadDeadline(time.Now().Add(3 * time.Second))
		var f brokerFrame
		if bus.Read(&f) == nil {
			frames <- f
		}
		close(frames)
	}()
	result := make(chan error, 1)
	go func() {
		_, err := o.action(context.Background(), action, json.RawMessage(args), json.RawMessage(`{"threadId":"one"}`))
		result <- err
	}()
	select {
	case f, ok := <-frames:
		t.Fatalf("%s %s reached the daemon: %s %s (%v)", action, args, f["method"], f["params"], ok)
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("%s %s = %v", action, args, err)
		}
	}
	go func() {
		_, _ = o.action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"after@local","source":"/abs/after.txt"}`), json.RawMessage(`{"threadId":"one"}`))
	}()
	f, ok := <-frames
	if !ok || string(f["method"]) != `"transfer.offer"` || string(f["params"]) != `{"target":"after@local","name":"after.txt"}` {
		t.Fatalf("first daemon frame after %s %s = %s %s (%v)", action, args, f["method"], f["params"], ok)
	}
}

// Through the real broker and SDK, another spelling of a path key is refused
// before the daemon sees it, with a known or an unknown thread cwd. Before
// this change it reached the daemon and the SDK opened the path relative to
// this process. The canonical key reaches the executor bound to the cwd.
func TestBrokerTransferRefusesAnotherSpellingOfAPathKey(t *testing.T) {
	for _, test := range []struct{ action, args string }{
		{"transfer.offer", `{"target":"bad@local","Source":"notes.txt"}`},
		{"transfer.offer", `{"target":"bad@local","ſource":"notes.txt"}`},
		{"transfer.offer", `{"target":"bad@local","source":"/abs/notes.txt","Source":"notes.txt"}`},
		{"transfer.request", `{"target":"bad@local","what":"w","Destination":"out.txt","overwrite":true}`},
	} {
		for _, cwd := range []string{t.TempDir(), ""} {
			o, _, _, bus, _, _ := transferBroker(t, cwd, &recordingTransferExecutor{}, false)
			pathKeyRefused(t, o, bus, test.action, test.args)
		}
	}

	cwd := t.TempDir()
	if err := os.WriteFile(filepath.Join(cwd, "notes.txt"), []byte("sent body\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	executor := &recordingTransferExecutor{real: true}
	o, _, _, bus, _, _ := transferBroker(t, cwd, executor, false)
	offerAndPrepare(t, o, bus, "tr-canonical", len("sent body\n"))
	if got := executor.recorded(); len(got) != 1 || got[0] != filepath.Join(cwd, "notes.txt") {
		t.Fatalf("canonical source opened = %q", got)
	}
}

// laneTransferOwner gives a lane tool owner a transfer-capable caller on an
// SDK connection whose daemon side is bus.
func laneTransferOwner(t *testing.T, executor kit.TransferExecutor) (*laneToolOwner, *Wrapper, *muxPipe) {
	t.Helper()
	transfers, err := kit.NewConnectionTransfers(executor)
	if err != nil {
		t.Fatal(err)
	}
	client, daemon := net.Pipe()
	bus := &muxPipe{Conn: daemon, dec: json.NewDecoder(daemon)}
	ready := make(chan struct{})
	var wire *kit.Connection
	wire = kit.NewConnection(client, func(_ context.Context, request *kit.Request) {
		<-ready
		transfers.HandleConnectionRequest(wire, request)
	})
	close(ready)
	t.Cleanup(func() { _ = wire.Close(); _ = bus.Close() })
	hello := make(chan error, 1)
	go func() {
		hello <- transfers.HelloObserved(context.Background(), context.Background(), wire,
			kit.PeerIdentity{SessionID: "lane@local", Product: Product, Groups: []string{"launch"}}, nil)
	}()
	request := receiveNative(t, bus)
	if string(request["method"]) != `"session.hello"` {
		t.Fatalf("hello = %s", request["method"])
	}
	writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": request["id"], "result": brokerRaw(map[string]any{"capabilities": []string{protocol.TransferCapability}})})
	if err := <-hello; err != nil {
		t.Fatal(err)
	}
	p := New()
	p.SetCaller(kit.NewCallerWithTransfers(func(context.Context, string, any) (json.RawMessage, error) {
		return nil, errors.New("unexpected non-transfer call")
	}, transfers))
	return &laneToolOwner{owner: p}, p, bus
}

// A lane forwards the bound path to the SDK: with no thread cwd a relative
// source is refused before the SDK, and with one the executor opens the
// thread-absolute path at prepare. Another spelling of the key is refused
// before the daemon sees it; before this change it reached the daemon.
func TestLaneToolForwardsTheBoundTransferPath(t *testing.T) {
	executor := &recordingTransferExecutor{}
	owner, p, bus := laneTransferOwner(t, executor)
	ctx := context.Background()
	if _, err := owner.Action(ctx, "transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`)); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("relative path before open = %v", err)
	}
	p.mu.Lock()
	p.cwd = "/lane/work"
	p.mu.Unlock()

	frames := make(chan brokerFrame, 1)
	go func() {
		_ = bus.SetReadDeadline(time.Now().Add(3 * time.Second))
		var f brokerFrame
		if bus.Read(&f) == nil {
			frames <- f
		}
		close(frames)
	}()
	refused := make(chan error, 1)
	go func() {
		_, err := owner.Action(ctx, "transfer.offer", json.RawMessage(`{"target":"bad","Source":"a.txt"}`))
		refused <- err
	}()
	select {
	case f, ok := <-frames:
		t.Fatalf("another spelling reached the daemon: %s %s (%v)", f["method"], f["params"], ok)
	case err := <-refused:
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("another spelling = %v", err)
		}
	}

	offered := make(chan error, 1)
	go func() {
		_, err := owner.Action(ctx, "transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`))
		offered <- err
	}()
	offer, ok := <-frames
	if !ok || string(offer["method"]) != `"transfer.offer"` || string(offer["params"]) != `{"target":"t","name":"a.txt"}` {
		t.Fatalf("lane offer = %s %s (%v)", offer["method"], offer["params"], ok)
	}
	descriptor := map[string]any{"id": "tr-lane", "sender": "lane@local", "receiver": "t", "name": "a.txt", "created_by": "lane@local", "state": "offered", "bytes": 0}
	writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": offer["id"], "result": brokerRaw(descriptor)})
	if err := <-offered; err != nil {
		t.Fatal(err)
	}
	writeBus(t, bus, frame("transfer.prepare", 900, map[string]any{"id": "tr-lane", "role": "source"}))
	if reply := receiveNative(t, bus); string(reply["id"]) != "900" {
		t.Fatalf("prepare reply = %s %s %s", reply["id"], reply["result"], reply["error"])
	}
	if got := executor.recorded(); len(got) != 1 || got[0] != "/lane/work/a.txt" {
		t.Fatalf("lane source opened = %q", got)
	}
}
