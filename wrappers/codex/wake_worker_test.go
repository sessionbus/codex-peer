// SPDX-License-Identifier: MIT
package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/testsocket"
)

type seededProduct struct {
	*Wrapper
	entered, release chan struct{}
	returned         chan error
}

func (p *seededProduct) Open(context.Context, kit.OpenRequest) (kit.OpenResult, error) {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.opened = true
	return kit.OpenResult{SessionID: p.id}, nil
}
func (p *seededProduct) Run(ctx context.Context, r *kit.Run, input kit.RunInput) (kit.TurnResult, error) {
	report := r.ReportDelivery
	if p.entered != nil {
		report = func(receipt kit.DeliveryReceipt, err error) error {
			close(p.entered)
			<-p.release
			return r.ReportDelivery(receipt, err)
		}
	}
	result, err := p.executeRun(ctx, r, input, report)
	p.returned <- err
	return result, err
}
func TestCodexActualWorkerSeedReceiptTerminalAndCollection(t *testing.T) {
	for _, mode := range []string{"direct", "terminal-before-receipt", "bus-loss-at-receipt", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			t.Setenv("SESSIONBUS_SOCKET", listener.Addr().String())
			t.Setenv("SESSIONBUS_LAUNCH_TOKEN", "seed-test")
			t.Setenv("SESSIONBUS_LOCAL_KEY", "")
			base, native := testLane(t)
			p := &seededProduct{Wrapper: base, returned: make(chan error, 1)}
			if mode == "terminal-before-receipt" || mode == "bus-loss-at-receipt" {
				p.entered = make(chan struct{})
				p.release = make(chan struct{})
			}
			worker := kit.NewWorker(p)
			p.SetCaller(worker.Caller())
			served := make(chan error, 1)
			go func() { served <- worker.Serve(context.Background()) }()
			c, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			reader := bufio.NewReader(c)
			next := func() protocol.Frame {
				t.Helper()
				line, e := reader.ReadBytes('\n')
				if e != nil {
					t.Fatal(e)
				}
				frame, e := protocol.DecodeFrame(line[:len(line)-1])
				if e != nil {
					t.Fatal(e)
				}
				return frame
			}
			send := func(body []byte, e error) {
				t.Helper()
				if e != nil {
					t.Fatal(e)
				}
				if _, e = c.Write(body); e != nil {
					t.Fatal(e)
				}
			}
			hello := next()
			send(protocol.ResultBytes(hello.ID, "session.hello", struct{}{}))
			send(protocol.RequestBytes(1, "session.open", kit.OpenRequest{Name: "seed@local", Groups: []string{}, Policy: &kit.LanePolicy{}}))
			if opened := next(); opened.Error != nil {
				t.Fatal(opened)
			}
			delivery := kit.DeliveryRequest{MessageID: "m", RunID: "g/1", Body: "wake-marker", From: kit.DeliverySource{SessionID: "parent@local", Product: "codex-peer", Groups: []string{"g"}}}
			send(protocol.RequestBytes(2, "message.deliver", delivery))
			start := readAppRequest(t, native)
			var params struct {
				ThreadID string
				Input    []struct{ Text string }
			}
			if start.Method != "turn/start" || json.Unmarshal(start.Params, &params) != nil || params.ThreadID != "thread-1" || len(params.Input) != 1 {
				t.Fatal(start)
			}
			writeApp(t, native, map[string]any{"id": start.ID, "result": map[string]any{"turn": map[string]string{"id": "native-turn"}}})
			writeRaw(t, native, `{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"native-turn","status":"inProgress"}}}`)
			if p.entered != nil {
				<-p.entered
			}
			terminal := map[string]any{"id": "native-turn", "status": "completed", "completedAt": 1, "items": []any{map[string]string{"type": "agentMessage", "phase": "final_answer", "text": "wake-answer"}}}
			if mode == "unavailable" {
				terminal["items"] = []any{}
			}
			writeApp(t, native, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": terminal}})
			// A following response executes an observer on the native reader, proving
			// terminal dispatch completed while the public receipt callback was held.
			barrier := make(chan error, 1)
			go func() { barrier <- base.app.call(context.Background(), "barrier", struct{}{}, nil) }()
			listHandled := false
			b := readAppRequest(t, native)
			if b.Method == "thread/turns/list" {
				listHandled = true
				writeApp(t, native, map[string]any{"id": b.ID, "result": map[string]any{"data": []any{terminal}}})
				b = readAppRequest(t, native)
			}
			if b.Method != "barrier" {
				t.Fatal(b)
			}
			writeApp(t, native, map[string]any{"id": b.ID, "result": map[string]any{}})
			if e := <-barrier; e != nil {
				t.Fatal(e)
			}
			if mode == "bus-loss-at-receipt" {
				_ = c.Close()
				<-worker.Closed()
				close(p.release)
				if e := <-p.returned; e == nil {
					t.Fatal("lost bus receipt succeeded")
				}
				<-served
				return
			}
			if p.release != nil {
				close(p.release)
			}
			receipt := next()
			var r kit.DeliveryReceipt
			if receipt.ID != 2 || protocol.UnmarshalResult("message.deliver", receipt.Result, &r) != nil || r.Disposition != "injected" {
				t.Fatal(receipt)
			}
			if p.release != nil {
				list := readAppRequest(t, native)
				if list.Method != "thread/turns/list" {
					t.Fatal(list)
				}
				writeApp(t, native, map[string]any{"id": list.ID, "result": map[string]any{"data": []any{terminal}}})
			} else if !listHandled {
				// The terminal read can be submitted after the barrier; service it now.
				list := readAppRequest(t, native)
				if list.Method != "thread/turns/list" {
					t.Fatal(list)
				}
				writeApp(t, native, map[string]any{"id": list.ID, "result": map[string]any{"data": []any{terminal}}})
			}
			ready := next()
			if ready.Method != "turn.ready" {
				t.Fatal(ready)
			}
			send(protocol.ResultBytes(ready.ID, "turn.ready", struct{}{}))
			for _, id := range []int64{3, 4} {
				send(protocol.RequestBytes(id, "turn.status", kit.ReadRequest{SessionID: "thread-1@local", RunID: "g/1"}))
				frame := next()
				var status kit.RunStatus
				if protocol.UnmarshalResult("turn.status", frame.Result, &status) != nil {
					t.Fatal(frame)
				}
				if mode == "unavailable" {
					if status.State != "unavailable" || status.Result != nil {
						t.Fatal(status)
					}
				} else if status.Result == nil || status.Result.Result != "wake-answer" {
					t.Fatal(status)
				}
			}
			send(protocol.RequestBytes(5, "turn.ack", kit.RunRef{SessionID: "thread-1@local", RunID: "g/1"}))
			if ack := next(); ack.Error != nil {
				t.Fatal(ack)
			}
			e := <-p.returned
			if mode != "unavailable" && e != nil {
				t.Fatal(e)
			}
			_ = c.Close()
			<-served
		})
	}
}
