// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
)

type PeerBackend struct {
	mu            sync.Mutex
	prepareMu     sync.Mutex
	app           *appClient
	peer          *sessionkit.Peer
	caller        *sessionkit.Caller
	identity      sessionkit.PeerIdentity
	groups        []string
	fixedID       string
	requestedName string
	named         string
	shutdown      func()
}

var _ mcp.Backend = (*PeerBackend)(nil)

func NewPeerBackend(_ context.Context) (*PeerBackend, error) {
	groups := []string{}
	if raw := os.Getenv(host.GroupsEnv); raw != "" && json.Unmarshal([]byte(raw), &groups) != nil {
		return nil, errors.New("SESSIONBUS_GROUPS must be a JSON array")
	}
	b := &PeerBackend{groups: groups, fixedID: strings.TrimSpace(os.Getenv(host.SessionIDEnv)), requestedName: strings.TrimSpace(os.Getenv(host.NameEnv))}
	b.caller = sessionkit.NewCaller(b.Call)
	return b, nil
}

var peerAppDial = dialPeerApp

func (b *PeerBackend) start(ctx context.Context) error {
	b.mu.Lock()
	started := b.app != nil
	b.mu.Unlock()
	if started {
		return nil
	}
	transport, err := peerAppDial(ctx)
	if err != nil {
		return err
	}
	app := newTransportClient(transport, nil, b.nativeFailure)
	b.mu.Lock()
	b.app = app
	b.mu.Unlock()
	if err = app.initialize(ctx, "Sessionbus Codex Peer"); err != nil {
		_ = app.close()
		b.mu.Lock()
		if b.app == app {
			b.app = nil
		}
		b.mu.Unlock()
	}
	return err
}

func (b *PeerBackend) Caller() *sessionkit.Caller { return b.caller }
func (b *PeerBackend) SetShutdown(shutdown func()) {
	b.mu.Lock()
	b.shutdown = shutdown
	b.mu.Unlock()
}

func (b *PeerBackend) Prepare(ctx context.Context, meta json.RawMessage) error {
	b.prepareMu.Lock()
	defer b.prepareMu.Unlock()
	var observed struct {
		ThreadID string `json:"threadId"`
	}
	if len(meta) != 0 && string(meta) != "null" && json.Unmarshal(meta, &observed) != nil {
		return errors.New("Codex peer metadata is invalid")
	}
	id := strings.TrimSpace(observed.ThreadID)
	b.mu.Lock()
	if b.fixedID != "" {
		if id != "" && id != b.fixedID {
			b.mu.Unlock()
			return errors.New("Codex peer thread identity changed")
		}
		id = b.fixedID
	}
	current := b.identity.SessionID
	b.mu.Unlock()
	if id == "" {
		return errors.New("Codex peer identity is unavailable; start Codex with codex-peer")
	}
	if current != "" && id != current {
		return errors.New("Codex peer thread identity changed")
	}
	if err := b.start(ctx); err != nil {
		return err
	}
	thread, err := b.readThread(ctx, id)
	if err != nil {
		return err
	}
	// Prepare runs inside the active turn that is waiting for this tool call.
	// thread/read is the only native call allowed here; resume and name/set can
	// wait for that turn and therefore belong only to out-of-band delivery.
	identity := sessionkit.PeerIdentity{Product: Product, SessionID: id, Name: first(b.requestedName, strings.TrimSpace(thread.Name), id), Groups: append([]string{}, b.groups...), Info: map[string]any{"cwd": strings.TrimSpace(thread.Cwd)}}
	b.mu.Lock()
	peer, old := b.peer, b.identity
	b.mu.Unlock()
	if peer == nil {
		return b.connect(ctx, identity)
	}
	if old.Name == identity.Name && old.Info["cwd"] == identity.Info["cwd"] {
		return nil
	}
	if err = peer.Rehello(ctx, identity.Name, identity.Info); err != nil {
		return err
	}
	b.mu.Lock()
	b.identity = identity
	b.mu.Unlock()
	return nil
}

func (b *PeerBackend) connect(ctx context.Context, identity sessionkit.PeerIdentity) error {
	peer, err := sessionkit.ConnectPeer(identity, b.deliver)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		peer.Shutdown()
		return ctx.Err()
	case <-peer.Ready():
	case <-peer.Closed():
		if err = peer.Err(); err != nil {
			return err
		}
		return errors.New("Codex peer closed before ready")
	}
	b.mu.Lock()
	b.peer, b.identity = peer, identity
	b.mu.Unlock()
	return nil
}

func (b *PeerBackend) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	b.mu.Lock()
	peer := b.peer
	b.mu.Unlock()
	if peer == nil {
		return nil, errors.New("Codex peer identity is unavailable; start Codex with codex-peer")
	}
	return peer.Call(ctx, method, params)
}

func (b *PeerBackend) nativeFailure(error) {
	b.mu.Lock()
	peer := b.peer
	b.mu.Unlock()
	if peer != nil {
		peer.Shutdown()
	}
}

func (b *PeerBackend) readThread(ctx context.Context, id string) (nativeThread, error) {
	var result threadReply
	if err := b.app.call(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false}, &result); err != nil {
		return nativeThread{}, err
	}
	if result.Thread.ID != id {
		return nativeThread{}, fmt.Errorf("Codex App Server returned thread %q, expected %q", result.Thread.ID, id)
	}
	if filepath.Base(filepath.Dir(result.Thread.Path)) == "archived_sessions" {
		b.mu.Lock()
		shutdown := b.shutdown
		b.mu.Unlock()
		if shutdown != nil {
			shutdown()
		}
		return nativeThread{}, errors.New("Codex peer thread is archived")
	}
	return result.Thread, nil
}

func (b *PeerBackend) loadThread(ctx context.Context, id string) (nativeThread, string, error) {
	thread, err := b.readThread(ctx, id)
	if err != nil {
		return nativeThread{}, "", err
	}
	status, err := statusType(thread.Status)
	if err != nil || status != "notLoaded" {
		return thread, status, err
	}
	var result threadReply
	if err = b.app.call(ctx, "thread/resume", map[string]any{"threadId": id, "excludeTurns": true}, &result); err != nil {
		return nativeThread{}, "", err
	}
	if result.Thread.ID != id {
		return nativeThread{}, "", fmt.Errorf("Codex App Server resumed thread %q, expected %q", result.Thread.ID, id)
	}
	status, err = statusType(result.Thread.Status)
	if err != nil {
		return nativeThread{}, "", err
	}
	if status != "idle" {
		return nativeThread{}, "", fmt.Errorf("Codex App Server resumed thread with status %q, expected idle", status)
	}
	return result.Thread, status, nil
}

func (b *PeerBackend) deliver(ctx context.Context, admitted sessionkit.PeerIdentity, request sessionkit.DeliveryRequest) (receipt sessionkit.DeliveryReceipt, err error) {
	defer func() {
		if ctx.Err() != nil {
			receipt, err = sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "closing"}, nil
		}
	}()
	message, err := host.RenderNativeMessage(request)
	if err != nil {
		return sessionkit.DeliveryReceipt{}, err
	}
	id := admitted.SessionID
	_, status, err := b.loadThread(ctx, id)
	if err != nil {
		return sessionkit.DeliveryReceipt{}, err
	}
	if status == "idle" && b.requestedName != "" {
		b.mu.Lock()
		named := b.named == id
		b.mu.Unlock()
		if !named && b.app.call(ctx, "thread/name/set", map[string]string{"threadId": id, "name": b.requestedName}, &struct{}{}) == nil {
			b.mu.Lock()
			b.named = id
			b.mu.Unlock()
		}
	}
	if status == "active" {
		var page struct {
			Data []nativeTurn `json:"data"`
		}
		if err = b.app.call(ctx, "thread/turns/list", map[string]any{"threadId": id, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"}, &page); err != nil {
			return sessionkit.DeliveryReceipt{}, err
		}
		if len(page.Data) != 1 || page.Data[0].ID == "" || page.Data[0].Status != "inProgress" {
			return sessionkit.DeliveryReceipt{}, errors.New("Codex active turn is unavailable")
		}
		var steered struct {
			TurnID string `json:"turnId"`
		}
		err = b.app.call(ctx, "turn/steer", map[string]any{"threadId": id, "expectedTurnId": page.Data[0].ID, "input": textInput(message)}, &steered)
		if err == nil && steered.TurnID != page.Data[0].ID {
			err = errors.New("Codex steered a different turn")
		}
	} else {
		var started turnReply
		err = b.app.call(ctx, "turn/start", map[string]any{"threadId": id, "input": textInput(message)}, &started)
		if err == nil && started.Turn.ID == "" {
			err = errors.New("Codex did not start a delivery turn")
		}
	}
	return sessionkit.DeliveryReceipt{Disposition: "injected"}, err
}

func statusType(raw json.RawMessage) (string, error) {
	var object struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &object) != nil || !slices.Contains([]string{"active", "idle", "notLoaded", "systemError"}, object.Type) {
		return "", errors.New("Codex App Server returned an unsupported thread status")
	}
	return object.Type, nil
}

func (b *PeerBackend) Shutdown() {
	b.mu.Lock()
	peer, app := b.peer, b.app
	b.peer = nil
	b.mu.Unlock()
	if peer != nil {
		peer.Shutdown()
		<-peer.Closed()
	}
	if app != nil {
		_ = app.close()
	}
}
