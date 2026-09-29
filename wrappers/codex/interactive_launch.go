// SPDX-License-Identifier: MIT

package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

var brokerNativeCommand = exec.Command

type brokerLaunch struct {
	Parent                       int
	Native, Dir, BusSocket, Name string
	Config, Groups               []string
}

func nativeCodexPath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(cwd, dir)
		}
		path := filepath.Join(dir, "codex")
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() && syscall.Access(path, 1) == nil {
			return path, nil
		}
	}
	return "", errors.New("native codex executable was not found on PATH")
}
func cleanInteractiveEnvironment(env []string) []string {
	result := make([]string, 0, len(env)+1)
	for _, value := range env {
		name, _, _ := strings.Cut(value, "=")
		if strings.HasPrefix(name, "SESSIONBUS_") {
			continue
		}
		result = append(result, value)
	}
	return result
}

func brokerEnvironment(env []string, endpoint string) []string {
	// Native MCP receives only its explicitly allowlisted endpoint; identity and
	// launch groups live in the owning broker, never ambient inherited IDs.
	return append(cleanInteractiveEnvironment(env), EndpointEnv+"="+endpoint)
}

// LaunchInteractive starts the direct child broker, then replaces this process
// with native Codex. The same OS parent identity remains across exec.
func LaunchInteractive(ctx context.Context, args []string) error {
	if os.Getenv("SESSIONBUS_LAUNCH_TOKEN") != "" {
		return errors.New("interactive launcher cannot consume a lane token")
	}
	if len(args) == 1 && args[0] == "--version" {
		native, err := nativeCodexPath()
		if err != nil {
			return err
		}
		return syscall.Exec(native, []string{native, "--version"}, cleanInteractiveEnvironment(os.Environ()))
	}
	options, err := parseInteractiveOptions(args)
	if err != nil {
		return err
	}
	if _, err = InstalledCodexPlugin(); err != nil {
		return err
	}
	native, err := nativeCodexPath()
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "sessionbus-codex-launch-")
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(dir)
		}
	}()
	configRead, configWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer configRead.Close()
	defer configWrite.Close()
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer readyRead.Close()
	defer readyWrite.Close()
	child := exec.Command(executable)
	child.Args = []string{BrokerAlias}
	child.ExtraFiles = []*os.File{configRead, readyWrite}
	child.Stderr = os.Stderr
	child.Env = brokerEnvironment(os.Environ(), filepath.Join(dir, "owner.sock"))
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = child.Start(); err != nil {
		return err
	}
	_ = configRead.Close()
	_ = readyWrite.Close()
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	// Cancellation or failed exec ends only the owned broker, never another TUI.
	stop := context.AfterFunc(ctx, func() { _ = child.Process.Signal(syscall.SIGTERM); _ = readyRead.Close() })
	defer stop()
	launch := brokerLaunch{Parent: os.Getpid(), Native: native, Dir: dir, BusSocket: kit.Socket(), Config: options.config, Groups: options.groups, Name: options.name}
	err = json.NewEncoder(configWrite).Encode(launch)
	_ = configWrite.Close()
	if err == nil {
		var response struct {
			Ready bool
			Error string
		}
		err = json.NewDecoder(readyRead).Decode(&response)
		if err == nil && !response.Ready {
			err = errors.New(response.Error)
		}
	}
	if err != nil {
		_ = child.Process.Signal(syscall.SIGTERM)
		return errors.Join(err, <-exited)
	}
	if err = ctx.Err(); err != nil {
		_ = child.Process.Signal(syscall.SIGTERM)
		return errors.Join(err, <-exited)
	}
	prefix := append(ActivationArguments(), "--remote", "unix://"+filepath.Join(dir, "tui.sock"))
	argv := append([]string{native}, append(prefix, options.native...)...)
	values := brokerEnvironment(os.Environ(), filepath.Join(dir, "owner.sock"))
	if err = syscall.Exec(native, argv, values); err != nil {
		_ = child.Process.Signal(syscall.SIGTERM)
		return errors.Join(err, <-exited)
	}
	cleanup = false // Exec never returns on success; broker now owns endpoint cleanup.
	return nil
}

// RunInteractiveBroker is dispatched only by its private basename. FD 3 is a
// launch record from our direct parent, FD 4 the one-shot readiness response.
func RunInteractiveBroker(ctx context.Context) error {
	input := os.NewFile(3, "broker-launch")
	ready := os.NewFile(4, "broker-ready")
	if input == nil || ready == nil {
		return errors.New("broker launch descriptors are missing")
	}
	defer input.Close()
	defer ready.Close()
	var launch brokerLaunch
	if err := json.NewDecoder(io.LimitReader(input, 1<<20)).Decode(&launch); err != nil {
		return err
	}
	_ = input.Close()
	err := runInteractiveBroker(ctx, launch, ready)
	if err != nil {
		_ = json.NewEncoder(ready).Encode(map[string]any{"Ready": false, "Error": err.Error()})
	}
	return err
}

func runInteractiveBroker(ctx context.Context, launch brokerLaunch, ready io.Writer) error {
	parent, stop, err := watchBrokerParent(launch.Parent)
	if err != nil {
		return err
	}
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-parent:
			cancel()
		case <-ctx.Done():
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	// Launch paths are generated by the parent; only this private directory is
	// removed. Never replace another listener or unlink a live shared socket.
	if !filepath.IsAbs(launch.Dir) || !strings.HasPrefix(filepath.Base(launch.Dir), "sessionbus-codex-launch-") {
		return errors.New("invalid broker launch directory")
	}
	defer os.RemoveAll(launch.Dir)
	owners := newBrokerOwners(ctx, launch.Groups, launch.BusSocket)
	owners.initialName = launch.Name
	endpoint, err := newBrokerEndpoint(launch.Dir, owners)
	if err != nil {
		return err
	}
	defer endpoint.Close()
	listener, err := netListenBroker(filepath.Join(launch.Dir, "tui.sock"))
	if err != nil {
		return err
	}
	defer listener.Close()
	command := brokerNativeCommand(launch.Native, append(append([]string{"app-server", "--stdio"}, ActivationArguments()...), launch.Config...)...)
	command.Env = brokerEnvironment(os.Environ(), endpoint.path)
	// An explicit drainer owns stderr independently of native request handling.
	stderr, stderrWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	command.Stderr = stderrWrite
	child, nativeInput, nativeOutput, err := startNative(command)
	_ = stderrWrite.Close()
	if err != nil {
		_ = stderr.Close()
		return err
	}
	drainedStderr := make(chan struct{})
	go func() { defer stderr.Close(); _, _ = io.Copy(os.Stderr, stderr); close(drainedStderr) }()
	stream := &brokerStdio{input: nativeInput, reader: bufio.NewReader(nativeOutput)}
	mux := newBrokerMux(ctx, stream, owners.observe, owners.selectThread)
	owners.setMux(mux)
	defer owners.End()
	_, served := serveBrokerTUI(mux, listener)
	go func() {
		select {
		case <-child.Done():
			mux.fail(errors.New("native App Server exited"))
		case <-mux.ctx.Done():
		}
	}()
	if err = mux.ctx.Err(); err == nil {
		err = json.NewEncoder(ready).Encode(map[string]bool{"Ready": true})
	}
	if err != nil {
		mux.fail(err)
	}
	<-mux.ctx.Done()
	owners.End()
	_ = endpoint.Close()
	<-served
	// The sole stdin writer is closed by mux.fail. Drain remaining native output
	// after the reader exits, then join the direct native process without timeout.
	<-mux.done
	_, _ = io.Copy(io.Discard, stream.reader)
	_ = nativeOutput.Close()
	childErr := child.Wait()
	<-drainedStderr
	if err != nil {
		return err
	}
	if childErr != nil {
		return fmt.Errorf("native App Server exit: %w", childErr)
	}
	return nil
}
