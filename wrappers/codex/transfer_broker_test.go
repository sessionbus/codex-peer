// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var transferPeer = kit.DeliverySource{SessionID: "peer@local", Product: "fixture", Groups: []string{"launch"}}

// transferBroker admits thread "one" with cwd on a transfer-capable fake
// daemon and returns that daemon's side of the connection and the hello it
// read. A non-nil executor replaces the thread's SDK transfer owner before
// its tools are ready; the publisher reads it only after taking r.mu.
func transferBroker(t *testing.T, cwd string, executor kit.TransferExecutor, immediateRetry bool) (*brokerOwners, *brokerMux, *muxPipe, *muxPipe, <-chan *muxPipe, brokerFrame) {
	t.Helper()
	o, m, p, buses := residentFixture(t)
	if immediateRetry {
		o.retry = func(ctx context.Context) bool { return ctx.Err() == nil }
	}
	nativeEvent(t, p, m, "thread/started", map[string]any{"thread": map[string]any{"id": "one", "cwd": cwd, "status": map[string]string{"type": "idle"}}})
	if executor != nil {
		transfers, err := kit.NewConnectionTransfers(executor)
		if err != nil {
			t.Fatal(err)
		}
		o.mu.Lock()
		r := o.owners["one"]
		o.mu.Unlock()
		r.mu.Lock()
		r.transfers = transfers
		r.mu.Unlock()
	}
	readyCatalog(t, m, p, "one")
	bus := receiveBus(t, buses)
	hello := admitTransferHello(t, o, bus)
	return o, m, p, bus, buses, hello
}

// admitTransferHello answers the resident's hello with the transfer
// capability and waits until the thread is admitted. The SDK attaches the
// connection's transfers before our admission callback runs, so admission
// implies attachment; the list call only sequences the test after that hello
// frame on the same reader.
func admitTransferHello(t *testing.T, o *brokerOwners, bus *muxPipe) brokerFrame {
	t.Helper()
	hello := receiveNative(t, bus)
	if string(hello["method"]) != `"session.hello"` {
		t.Fatalf("hello = %s", hello["method"])
	}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": hello["id"], "result": brokerRaw(map[string]any{"capabilities": []string{protocol.TransferCapability}})}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(time.Millisecond) {
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
	}
	listed := make(chan error, 1)
	go func() {
		_, err := o.action(context.Background(), "list", json.RawMessage(`{}`), json.RawMessage(`{"threadId":"one"}`))
		listed <- err
	}()
	list := receiveNative(t, bus)
	if string(list["method"]) != `"session.list"` {
		t.Fatalf("barrier = %s", list["method"])
	}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": list["id"], "result": brokerRaw(map[string]any{"sessions": []any{}})}); err != nil {
		t.Fatal(err)
	}
	if err := <-listed; err != nil {
		t.Fatal(err)
	}
	return hello
}

// offerAndPrepare offers cwd/notes.txt as transfer id and answers the
// daemon's source prepare, which the SDK serves by opening that file.
func offerAndPrepare(t *testing.T, o *brokerOwners, bus *muxPipe, id string, size int) {
	t.Helper()
	type actionResult struct {
		out json.RawMessage
		err error
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
	descriptor := map[string]any{"id": id, "sender": "one", "receiver": "peer@local", "name": "notes.txt", "created_by": "one", "state": "offered", "bytes": 0}
	if err := bus.Write(brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": offer["id"], "result": brokerRaw(descriptor)}); err != nil {
		t.Fatal(err)
	}
	if got := <-offered; got.err != nil || !strings.Contains(string(got.out), `"id":"`+id+`"`) {
		t.Fatalf("offer result = %s, %v", got.out, got.err)
	}
	if err := bus.Write(frame("transfer.prepare", 900, map[string]any{"id": id, "role": "source"})); err != nil {
		t.Fatal(err)
	}
	prepared := receiveNative(t, bus)
	var result struct {
		ID   string
		Size int
	}
	if string(prepared["id"]) != "900" || json.Unmarshal(prepared["result"], &result) != nil || result.ID != id || result.Size != size {
		t.Fatalf("prepare reply = %s %s", prepared["result"], prepared["error"])
	}
}

// writeBus fails, rather than hangs, when the resident stops reading.
func writeBus(t *testing.T, bus *muxPipe, f brokerFrame) {
	t.Helper()
	_ = bus.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := bus.Write(f); err != nil {
		t.Fatalf("the resident did not read %s: %v", f["method"], err)
	}
}

// The final notice for a received file waits for the transfer's resources to
// retire. That retirement needs the machine terminal notice, which arrives on
// the same reader, so the projection must not hold the reader: a later
// ordinary delivery and the machine notice both progress while it waits.
func TestBrokerHeldFinalNoticeProjectionKeepsTheReaderMoving(t *testing.T) {
	cwd := t.TempDir()
	content := []byte("received body\n")
	digest := sha256.Sum256(content)
	sum := hex.EncodeToString(digest[:])
	size := len(content)
	o, _, _, bus, _, _ := transferBroker(t, cwd, nil, false)

	writeBus(t, bus, frame("transfer.notice", 10, map[string]any{"kind": "offer", "id": "tr-in", "from": transferPeer, "name": "in.bin", "size": size}))
	if ack := receiveNative(t, bus); string(ack["id"]) != "10" || ack["error"] != nil {
		t.Fatalf("offer notice ack = %v", ack)
	}
	accepted := make(chan error, 1)
	go func() {
		_, err := o.action(context.Background(), "transfer.accept", json.RawMessage(`{"id":"tr-in","destination":"in.bin"}`), json.RawMessage(`{"threadId":"one"}`))
		accepted <- err
	}()
	accept := receiveNative(t, bus)
	if string(accept["method"]) != `"transfer.accept"` || string(accept["params"]) != `{"id":"tr-in"}` {
		t.Fatalf("wire accept = %s %s", accept["method"], accept["params"])
	}
	writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": accept["id"], "result": brokerRaw(map[string]any{"id": "tr-in", "sender": "peer@local", "receiver": "one", "name": "in.bin", "created_by": "peer@local", "state": "preparing", "bytes": 0})})
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	for i, step := range []struct {
		method string
		params map[string]any
	}{
		{"transfer.prepare", map[string]any{"id": "tr-in", "role": "destination", "size": size}},
		{"transfer.start", map[string]any{"id": "tr-in", "role": "destination", "size": size}},
		{"transfer.chunk", map[string]any{"id": "tr-in", "offset": 0, "data": base64.StdEncoding.EncodeToString(content)}},
		{"transfer.finish", map[string]any{"id": "tr-in", "bytes": size, "sha256": sum}},
	} {
		writeBus(t, bus, frame(step.method, 11+i, step.params))
		if reply := receiveNative(t, bus); string(reply["id"]) != brokerRawString(11+i) || reply["error"] != nil {
			t.Fatalf("%s reply = %s %s", step.method, reply["result"], reply["error"])
		}
	}
	destination := filepath.Join(cwd, "in.bin")
	if got, err := os.ReadFile(destination); err != nil || string(got) != string(content) {
		t.Fatalf("published file = %q, %v", got, err)
	}

	// The ordinary terminal notice precedes the machine one, so its projection
	// is held until the machine notice retires the transfer.
	terminal, _ := json.Marshal(map[string]any{"kind": "terminal", "id": "tr-in", "outcome": "completed", "bytes": size, "sha256": sum})
	writeBus(t, bus, frame("message.deliver", 20, kit.DeliveryRequest{MessageID: "transfer-notice-tr-in", From: transferPeer, Body: string(terminal)}))
	writeBus(t, bus, frame("message.deliver", 21, kit.DeliveryRequest{MessageID: "later", From: transferPeer, Body: "later message"}))
	if receipt := receiveNative(t, bus); string(receipt["id"]) != "21" || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatalf("first reply while the projection is held = %s %s %s", receipt["id"], receipt["result"], receipt["error"])
	}
	writeBus(t, bus, frame("transfer.notice", 22, map[string]any{"kind": "terminal", "id": "tr-in", "outcome": "completed", "bytes": size, "sha256": sum}))
	replies := map[string]brokerFrame{}
	for len(replies) < 2 {
		reply := receiveNative(t, bus)
		replies[string(reply["id"])] = reply
	}
	if ack := replies["22"]; ack == nil || ack["error"] != nil {
		t.Fatalf("machine terminal ack = %v", ack)
	}
	if receipt := replies["20"]; receipt == nil || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatalf("final notice receipt = %v", receipt)
	}
	o.mu.Lock()
	r := o.owners["one"]
	o.mu.Unlock()
	r.mu.Lock()
	queued := strings.Join(r.queue, "\n")
	r.mu.Unlock()
	if !strings.Contains(queued, "later message") || !strings.Contains(queued, `"destination":"`+destination+`"`) {
		t.Fatalf("queued deliveries = %s", queued)
	}
}

func brokerRawString(v any) string { return string(brokerRaw(v)) }

// gatedTransferExecutor opens real files and holds the first source Close
// until released, so a test can see whether cleanup was joined.
type gatedTransferExecutor struct {
	closing, release chan struct{}
	once, released   sync.Once
}

// open releases the held cleanup; a test registers it for cleanup too, so a
// failed control never leaves End joined to a gate that stays shut.
func (e *gatedTransferExecutor) open() { e.released.Do(func() { close(e.release) }) }

func newGatedTransferExecutor() *gatedTransferExecutor {
	return &gatedTransferExecutor{closing: make(chan struct{}), release: make(chan struct{})}
}
func (e *gatedTransferExecutor) OpenTransferSource(ctx context.Context, path string) (kit.TransferSource, error) {
	source, err := kit.FilesystemTransferExecutor{}.OpenTransferSource(ctx, path)
	if err != nil {
		return nil, err
	}
	return &gatedTransferSource{TransferSource: source, executor: e}, nil
}
func (e *gatedTransferExecutor) OpenTransferDestination(ctx context.Context, path string, overwrite bool) (kit.TransferDestination, error) {
	return kit.FilesystemTransferExecutor{}.OpenTransferDestination(ctx, path, overwrite)
}

type gatedTransferSource struct {
	kit.TransferSource
	executor *gatedTransferExecutor
}

func (s *gatedTransferSource) Close() error {
	s.executor.once.Do(func() { close(s.executor.closing) })
	<-s.executor.release
	return s.TransferSource.Close()
}

// openGatedSource admits thread "one" and opens one gated transfer source.
func openGatedSource(t *testing.T) (*brokerOwners, *brokerMux, *muxPipe, *muxPipe, <-chan *muxPipe, *gatedTransferExecutor) {
	t.Helper()
	cwd := t.TempDir()
	content := "gated body\n"
	if err := os.WriteFile(filepath.Join(cwd, "notes.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	gate := newGatedTransferExecutor()
	o, m, p, bus, buses, _ := transferBroker(t, cwd, gate, true)
	t.Cleanup(gate.open) // runs before the fixture's End
	offerAndPrepare(t, o, bus, "tr-held", len(content))
	return o, m, p, bus, buses, gate
}

// expectNoReconnectUntilJoined requires that the resident starts the old
// connection's cleanup and does not dial again while that cleanup is held,
// then reconnects once it is released; it returns the new connection.
func expectNoReconnectUntilJoined(t *testing.T, buses <-chan *muxPipe, gate *gatedTransferExecutor) *muxPipe {
	t.Helper()
	select {
	case <-gate.closing:
	case unexpected := <-buses:
		_ = unexpected.Close()
		t.Fatal("reconnected before the old connection's transfer cleanup started")
	case <-time.After(3 * time.Second):
		t.Fatal("the old connection's transfer cleanup did not start")
	}
	select {
	case unexpected := <-buses:
		_ = unexpected.Close()
		t.Fatal("reconnected before the old connection's transfer cleanup was joined")
	case <-time.After(200 * time.Millisecond):
	}
	gate.open()
	return receiveBus(t, buses)
}

// After a reconnect, the new hello keeps the thread's ID and the transfer
// bit, and a transfer action works on the new connection.
func expectTransferSuccessor(t *testing.T, o *brokerOwners, next *muxPipe, name string) {
	t.Helper()
	hello := admitTransferHello(t, o, next)
	var params struct {
		SessionID string         `json:"session_id"`
		Name      string         `json:"name"`
		Info      map[string]any `json:"info"`
	}
	if json.Unmarshal(hello["params"], &params) != nil || params.SessionID != "one" || params.Name != name || params.Info[protocol.TransferInfoKey] != float64(1) {
		t.Fatalf("successor hello = %s", hello["params"])
	}
	offerAndPrepare(t, o, next, "tr-next", len("gated body\n"))
}

// Ending the broker joins a thread's admitted transfer cleanup before End
// returns.
func TestBrokerEndJoinsAdmittedTransferCleanup(t *testing.T) {
	o, _, _, _, _, gate := openGatedSource(t)
	ended := make(chan struct{})
	go func() {
		o.End()
		close(ended)
	}()
	select {
	case <-gate.closing:
	case <-ended:
		t.Fatal("End returned before the transfer's source was closed")
	case <-time.After(3 * time.Second):
		t.Fatal("End did not start the transfer's cleanup")
	}
	select {
	case <-ended:
		t.Fatal("End returned before the transfer's cleanup was joined")
	case <-time.After(200 * time.Millisecond):
	}
	gate.open()
	select {
	case <-ended:
	case <-time.After(3 * time.Second):
		t.Fatal("End did not return after the cleanup was released")
	}
}

// A lost daemon connection's admitted transfer cleanup is joined before the
// thread reconnects; the successor connection keeps identity and transfers.
func TestBrokerReconnectJoinsTheLostConnectionsTransferCleanup(t *testing.T) {
	o, _, _, bus, buses, gate := openGatedSource(t)
	_ = bus.Close()
	next := expectNoReconnectUntilJoined(t, buses, gate)
	expectTransferSuccessor(t, o, next, "")
}

// A failed re-hello on a connection with an admitted transfer joins that
// connection's cleanup before reconnecting with the renamed identity.
func TestBrokerFailedRehelloJoinsTransferCleanupBeforeReconnect(t *testing.T) {
	o, m, p, bus, buses, gate := openGatedSource(t)
	nativeEvent(t, p, m, "thread/name/updated", map[string]any{"threadId": "one", "threadName": "renamed"})
	rehello := receiveNative(t, bus)
	if string(rehello["method"]) != `"session.hello"` || !strings.Contains(string(rehello["params"]), `"name":"renamed"`) {
		t.Fatalf("re-hello = %s %s", rehello["method"], rehello["params"])
	}
	writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": rehello["id"], "error": brokerRaw(map[string]any{"code": -32603, "message": "internal"})})
	next := expectNoReconnectUntilJoined(t, buses, gate)
	expectTransferSuccessor(t, o, next, "renamed")
}

// A sent file's final notice is projected too (a source cleanup failure would
// show there) and waits for the transfer to retire. Retirement needs both the
// finish reply and the machine terminal notice, which arrive on the same
// reader, so a later delivery and both of those still progress while the
// notice is held.
func TestBrokerHeldSourceFinalNoticeKeepsTheReaderMoving(t *testing.T) {
	cwd := t.TempDir()
	content := []byte("sent body\n")
	digest := sha256.Sum256(content)
	sum := hex.EncodeToString(digest[:])
	size := len(content)
	if err := os.WriteFile(filepath.Join(cwd, "notes.txt"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	o, _, _, bus, _, _ := transferBroker(t, cwd, nil, false)
	offerAndPrepare(t, o, bus, "tr-out", size)

	writeBus(t, bus, frame("transfer.start", 901, map[string]any{"id": "tr-out", "role": "source", "size": size, "credit": protocol.TransferCreditBytes}))
	var finish brokerFrame
	for started, chunked := false, false; !started || finish == nil; {
		f := receiveNative(t, bus)
		switch {
		case string(f["id"]) == "901" && f["method"] == nil:
			started = true
		case string(f["method"]) == `"transfer.chunk"` && !chunked:
			chunked = true
			writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": f["id"], "result": brokerRaw(map[string]any{"offset": size})})
		case string(f["method"]) == `"transfer.finish"`:
			finish = f
		default:
			t.Fatalf("unexpected frame while streaming = %s %s", f["method"], f["id"])
		}
	}

	// The ordinary terminal notice arrives while the finish reply and the
	// machine terminal notice are both still outstanding. Daemon request IDs
	// keep increasing on one connection.
	terminal, _ := json.Marshal(map[string]any{"kind": "terminal", "id": "tr-out", "outcome": "completed", "bytes": size, "sha256": sum})
	writeBus(t, bus, frame("message.deliver", 902, kit.DeliveryRequest{MessageID: "transfer-notice-tr-out", From: transferPeer, Body: string(terminal)}))
	writeBus(t, bus, frame("message.deliver", 903, kit.DeliveryRequest{MessageID: "later", From: transferPeer, Body: "later message"}))
	if receipt := receiveNative(t, bus); string(receipt["id"]) != "903" || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatalf("first reply while the projection is held = %s %s %s", receipt["id"], receipt["result"], receipt["error"])
	}
	writeBus(t, bus, brokerFrame{"jsonrpc": brokerRaw("2.0"), "id": finish["id"], "result": brokerRaw(map[string]any{"id": "tr-out", "outcome": "completed", "bytes": size, "sha256": sum})})
	writeBus(t, bus, frame("transfer.notice", 904, map[string]any{"kind": "terminal", "id": "tr-out", "outcome": "completed", "bytes": size, "sha256": sum}))
	replies := map[string]brokerFrame{}
	for len(replies) < 2 {
		reply := receiveNative(t, bus)
		replies[string(reply["id"])] = reply
	}
	if ack := replies["904"]; ack == nil || ack["error"] != nil {
		t.Fatalf("machine terminal ack = %v", ack)
	}
	if receipt := replies["902"]; receipt == nil || string(receipt["result"]) != `{"disposition":"written"}` {
		t.Fatalf("final notice receipt = %v", receipt)
	}
	o.mu.Lock()
	r := o.owners["one"]
	o.mu.Unlock()
	r.mu.Lock()
	queued := strings.Join(r.queue, "\n")
	r.mu.Unlock()
	if !strings.Contains(queued, "later message") || !strings.Contains(queued, `"id":"tr-out"`) || strings.Contains(queued, `"destination"`) {
		t.Fatalf("queued deliveries = %s", queued)
	}
}
