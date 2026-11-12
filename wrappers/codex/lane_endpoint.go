// SPDX-License-Identifier: MIT
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/sessionbus/peer-common/mcp"
)

type laneEndpoint struct {
	dir, path   string
	listener    net.Listener
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	closed      bool
	ready       chan struct{}
	readyOnce   sync.Once
}

func newLaneEndpoint(p *Wrapper) (*laneEndpoint, error) {
	dir, err := os.MkdirTemp("", "sessionbus-codex-")
	if err != nil {
		return nil, err
	}
	e := &laneEndpoint{dir: dir, path: filepath.Join(dir, "owner.sock"), connections: map[net.Conn]struct{}{}, ready: make(chan struct{})}
	e.listener, err = net.Listen("unix", e.path)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	go func() {
		for {
			c, err := e.listener.Accept()
			if err != nil {
				return
			}
			e.mu.Lock()
			if e.closed {
				e.mu.Unlock()
				_ = c.Close()
				return
			}
			e.connections[c] = struct{}{}
			e.mu.Unlock()
			go func() {
				_ = mcp.ServeSessionbus(&laneToolOwner{owner: p, endpoint: e}, c, c, mcp.ReportHandler{})
				_ = c.Close()
				e.mu.Lock()
				delete(e.connections, c)
				e.mu.Unlock()
			}()
		}
	}()
	return e, nil
}
func (e *laneEndpoint) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	_ = e.listener.Close()
	for c := range e.connections {
		_ = c.Close()
	}
	_ = os.RemoveAll(e.dir)
	return nil
}

type laneToolOwner struct {
	mu       sync.Mutex
	bound    bool
	owner    *Wrapper
	endpoint *laneEndpoint
}

func (b *laneToolOwner) validateCall(ctx context.Context, meta json.RawMessage) error {
	var native struct {
		ThreadID string `json:"threadId"`
	}
	if json.Unmarshal(meta, &native) != nil || native.ThreadID == "" {
		return errors.New("native tool call omitted threadId")
	}
	b.owner.mu.Lock()
	id, closing := b.owner.id, b.owner.closing
	b.owner.mu.Unlock()
	if id == "" || native.ThreadID != id || closing {
		return errors.New("native tool call does not belong to this open lane")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	b.bound = true
	b.mu.Unlock()
	return nil
}
func (b *laneToolOwner) ActionWithMeta(ctx context.Context, action string, args, meta json.RawMessage) (json.RawMessage, error) {
	if err := b.validateCall(ctx, meta); err != nil {
		return nil, err
	}
	return b.Action(ctx, action, args)
}
func (b *laneToolOwner) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	b.owner.mu.Lock()
	cwd := b.owner.cwd
	b.owner.mu.Unlock()
	args, err := mcp.BindTransferPaths(action, args, cwd)
	if err != nil {
		return nil, err
	}
	return b.owner.caller.Action(ctx, action, args)
}
func (b *laneToolOwner) End() {
	b.mu.Lock()
	bound := b.bound
	b.mu.Unlock()
	if bound {
		b.owner.transportEnd(errors.New("Codex MCP forwarder ended"))
	}
}

// MCP initialize confirms the endpoint was reached; native startup readiness and
// tool inventory remain separate gates. The engine calls this after responding.
func (b *laneToolOwner) Initialized() { b.endpoint.readyOnce.Do(func() { close(b.endpoint.ready) }) }

// ForwardLane carries native stdio verbatim to the sole worker endpoint.
func Forward(ctx context.Context, path string, input io.ReadCloser, output io.Writer) error {
	if path == "" {
		return errors.New("missing Codex lane endpoint")
	}
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { _ = c.Close(); _ = input.Close() })
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(c, input)
		if u, ok := c.(*net.UnixConn); ok {
			_ = u.CloseWrite()
		}
		done <- err
	}()
	_, err = io.Copy(output, c)
	_ = c.Close()
	_ = input.Close()
	return errors.Join(err, <-done)
}
