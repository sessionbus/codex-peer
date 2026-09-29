// SPDX-License-Identifier: MIT
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/sessionbus/codex-peer/wrappers/codex"
)

func TestInstalledPublicEntryAndPrivateBrokerSignal(t *testing.T) {
	// Only compiled stand-ins execute here, never native Codex or user config.
	root, err := os.MkdirTemp("/tmp", "cx-entry-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	t.Setenv("TMPDIR", root)
	t.Setenv("SESSIONBUS_LAUNCH_TOKEN", "")
	if err = os.Unsetenv("SESSIONBUS_LAUNCH_TOKEN"); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "package", "bin")
	nativeDir := filepath.Join(root, "native")
	for _, p := range []string{bin, nativeDir, filepath.Join(root, "plugin", ".codex-plugin")} {
		if err = os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	body, _ := json.Marshal(map[string]string{"pluginId": codex.PluginID, "installedPath": filepath.Join(root, "plugin")})
	if err = os.WriteFile(filepath.Join(root, "package", "installed.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "plugin", ".codex-plugin", "plugin.json"), []byte(`{"name":"codex"}`), 0600); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", filepath.Join(bin, "codex-peer"), ".")
	if b, e := build.CombinedOutput(); e != nil {
		t.Fatalf("production entry: %v %s", e, b)
	}
	source := `package main
import("os";"io";"encoding/json";"path/filepath";"fmt";"strings")
func main(){server:=len(os.Args)>1&&os.Args[1]=="app-server";name:="tui.json";if server{name="server.json"};names:=[]string{};for _,v:=range os.Environ(){n,_,_:=strings.Cut(v,"=");names=append(names,n)};dirs,_:=filepath.Glob(filepath.Join(os.Getenv("TMPDIR"),"sessionbus-codex-launch-*"));b,_:=json.Marshal(map[string]any{"pid":os.Getpid(),"argv":os.Args[1:],"env":names,"launchDirs":dirs});if e:=os.WriteFile(filepath.Join(os.Getenv("ENTRY_CAPTURE"),name),b,0600);e!=nil{panic(e)};if server{_,_=io.Copy(io.Discard,os.Stdin);fmt.Fprintln(os.Stderr,"NATIVE_EOF_JOINED");return};os.Exit(37)}`
	if err = os.WriteFile(filepath.Join(root, "native.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	build = exec.Command("go", "build", "-o", filepath.Join(nativeDir, "codex"), filepath.Join(root, "native.go"))
	if b, e := build.CombinedOutput(); e != nil {
		t.Fatalf("stand-in: %v %s", e, b)
	}
	public := filepath.Join(root, "codex-peer")
	if err = os.Symlink(filepath.Join(bin, "codex-peer"), public); err != nil {
		t.Fatal(err)
	}
	broker := filepath.Join(bin, codex.BrokerAlias)
	if err = os.Symlink("codex-peer", broker); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", nativeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("ENTRY_CAPTURE", root)
	t.Run("public-native-version-direct-exec", func(t *testing.T) {
		cmd := exec.Command(public, "--native-version")
		cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_SENTINEL=value", "SESSIONBUS_SOCKET=/ambient")
		b, e := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(e, &exit) || exit.ExitCode() != 37 {
			t.Fatalf("native status: %v %s", e, b)
		}
		var v struct {
			PID        int
			Argv       []string
			Env        []string
			LaunchDirs []string
		}
		raw, e := os.ReadFile(filepath.Join(root, "tui.json"))
		if e != nil || json.Unmarshal(raw, &v) != nil {
			t.Fatal(e, string(raw))
		}
		if v.PID != cmd.ProcessState.Pid() {
			t.Fatal("launcher retained a parent", v.PID, cmd.ProcessState.Pid())
		}
		if !reflect.DeepEqual(v.Argv, []string{"--version"}) {
			t.Fatal(v.Argv)
		}
		for _, name := range v.Env {
			if strings.HasPrefix(name, "SESSIONBUS_") {
				t.Fatal("native inherited Sessionbus environment", name)
			}
		}
		if len(v.LaunchDirs) != 0 {
			t.Fatal("native observed launch directories", v.LaunchDirs)
		}
		if _, e = os.Stat(filepath.Join(root, "server.json")); !os.IsNotExist(e) {
			t.Fatal("App Server capture exists", e)
		}
		if matches, e := filepath.Glob(filepath.Join(root, "sessionbus-codex-launch-*")); e != nil || len(matches) != 0 {
			t.Fatal("launch directory exists", matches, e)
		}
	})
	t.Run("public-exec-native-argv", func(t *testing.T) {
		cmd := exec.Command(public, "--resume", "native selector", "-g", "one", "--unknown-native", "literal value", "--group=two,three", "--yolo", "-c", "model=literal", "--model", "--resume", "-m", "--yolo", "--", "--group", "operand")
		b, e := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(e, &exit) || exit.ExitCode() != 37 {
			t.Fatalf("native status: %v %s", e, b)
		}
		if !bytes.Contains(b, []byte("NATIVE_EOF_JOINED")) {
			t.Fatalf("broker did not finish owned native after parent exit: %s", b)
		}
		var v struct {
			PID  int
			Argv []string
		}
		raw, e := os.ReadFile(filepath.Join(root, "tui.json"))
		if e != nil || json.Unmarshal(raw, &v) != nil {
			t.Fatal(e, string(raw))
		}
		if v.PID != cmd.ProcessState.Pid() {
			t.Fatal("launcher retained a parent", v.PID, cmd.ProcessState.Pid())
		}
		prefix := codex.ActivationArguments()
		if len(v.Argv) < len(prefix)+2 || !reflect.DeepEqual(v.Argv[:len(prefix)], prefix) || v.Argv[len(prefix)] != "--remote" {
			t.Fatal(v.Argv)
		}
		uri := v.Argv[len(prefix)+1]
		if !strings.HasPrefix(uri, "unix://"+root+"/sessionbus-codex-launch-") || !strings.HasSuffix(uri, "/tui.sock") {
			t.Fatal(uri)
		}
		want := []string{"resume", "native selector", "--unknown-native", "literal value", "--dangerously-bypass-approvals-and-sandbox", "-c", "model=literal", "--model", "--resume", "-m", "--yolo", "--", "--group", "operand"}
		if !reflect.DeepEqual(v.Argv[len(prefix)+2:], want) {
			t.Fatal(v.Argv)
		}
		raw, e = os.ReadFile(filepath.Join(root, "server.json"))
		if e != nil || json.Unmarshal(raw, &v) != nil {
			t.Fatal(e, string(raw))
		}
		want = append(append([]string{"app-server", "--stdio"}, prefix...), "-c", "model=literal")
		if !reflect.DeepEqual(v.Argv, want) {
			t.Fatal(v.Argv)
		}
		if _, e = os.Stat(strings.TrimPrefix(uri, "unix://")); !os.IsNotExist(e) {
			t.Fatal("listener remains", e)
		}
	})
	t.Run("private-TERM-is-cleanup", func(t *testing.T) {
		dir, err := os.MkdirTemp(root, "sessionbus-codex-launch-")
		if err != nil {
			t.Fatal(err)
		}
		input, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer input.Close()
		defer write.Close()
		ready, output, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer ready.Close()
		defer output.Close()
		cmd := exec.Command(broker)
		cmd.ExtraFiles = []*os.File{input, output}
		var log bytes.Buffer
		cmd.Stderr = &log
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		_ = input.Close()
		_ = output.Close()
		if err = json.NewEncoder(write).Encode(map[string]any{"Parent": os.Getpid(), "Native": filepath.Join(nativeDir, "codex"), "Dir": dir, "BusSocket": "/unused"}); err != nil {
			t.Fatal(err)
		}
		_ = write.Close()
		var response struct {
			Ready bool
			Error string
		}
		if err = json.NewDecoder(ready).Decode(&response); err != nil || !response.Ready {
			t.Fatal(response, err)
		}
		if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err = cmd.Wait(); err != nil {
			t.Fatal(err, log.String())
		}
		if !strings.Contains(log.String(), "NATIVE_EOF_JOINED") {
			t.Fatal(log.String())
		}
		if _, err = os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("owned directory remains", err)
		}
	})
}
