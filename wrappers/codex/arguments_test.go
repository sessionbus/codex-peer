// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/sessionbus/peer-common/host"
)

func TestProcessArguments(t *testing.T) {
	for _, args := range [][]string{
		{"--config", `model="gpt"`},
		{"--unknown", "value", "--", "--literal"},
		{"--enable"},
		{"app-server", "arbitrary"},
		{"-c", `mcp_servers.sessionbus.command=other`},
		{"--config", `plugins."other".enabled=false`},
		{"--config", `plugins."codex@sessionbus-peers".mcp_servers.other.command="other"`},
		{"--", "--config", `features.plugins=false`},
	} {
		got, err := processArguments(args)
		if err != nil || !slices.Equal(got, args) {
			t.Fatalf("args=%v got=%v err=%v", args, got, err)
		}
	}
}

func TestManagedConfigCannotBeReplacedByCaller(t *testing.T) {
	for _, arguments := range [][]string{
		{"-c", `features.plugins=false`},
		{`--config=plugins.codex@sessionbus-peers.enabled=false`},
		{`-cplugins.codex@sessionbus-peers.mcp_servers.sessionbus.tools.sessionbus.approval_mode="deny"`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers={}`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus={}`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled=false`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled_tools=[]`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled_tools=[sessionbus]`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.disabled_tools=["sessionbus"]`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.tools={}`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.tools.sessionbus.approval_mode="deny"`},
		{"--disable", "plugins"},
		{"--disable=plugins"},
	} {
		if _, err := processArguments(arguments); err == nil || !strings.Contains(err.Error(), "managed Sessionbus grant") {
			t.Fatalf("arguments=%q err=%v", arguments, err)
		}
	}
	for _, arguments := range [][]string{
		{"--config", `features.plugins_extra=false`},
		{"--config", `plugins."codex@sessionbus-peers".enabled=false`},
		{"--config", `plugins.'codex@sessionbus-peers'.enabled=false`},
		{"--config", `plugins."codex\u0040sessionbus-peers".enabled=false`},
		{"--config", `plugins.codex@sessionbus-peers-other.enabled=false`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.other.enabled=false`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.command="other"`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.default_tools_approval_mode="deny"`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.tools.other.approval_mode="deny"`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.tools.sessionbus.enabled=false`},
		{"--config", `"plugins.codex@sessionbus-peers".enabled=false`},
		{"-c", `mcp_servers.sessionbus.command=other`},
		{"--disable", "web_search_request"},
		{"--disable=unified_exec"},
		{"--", "--config", `features.plugins=false`, "--disable", "plugins"},
	} {
		if got, err := processArguments(arguments); err != nil || !slices.Equal(got, arguments) {
			t.Fatalf("unrelated arguments=%q got=%q err=%v", arguments, got, err)
		}
	}
}

func TestManagedConfigPreservesEquivalentEnables(t *testing.T) {
	for _, arguments := range [][]string{
		{"-c", `features.plugins=true`},
		{"-c", "features.plugins=true # retained enabling comment"},
		{"--config", `plugins.codex@sessionbus-peers.enabled = true`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled=true`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled_tools=["other", "sessionbus"]`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled_tools=['sessionbus']`},
		{"--config", "plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled_tools=[\"other\", # retained\n\"sessionbus\",]"},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.enabled_tools=["sessionbus"] # retained enabling comment`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.disabled_tools=[]`},
		{"--config", `plugins.codex@sessionbus-peers.mcp_servers.sessionbus.disabled_tools=["other"]`},
		{"--config", sessionbusApprovalConfigKey + `="approve"`},
		{"--config", sessionbusApprovalConfigKey + `='approve'`},
		{"--config", sessionbusApprovalConfigKey + `="appr\u006fve"`},
		{"--config", sessionbusApprovalConfigKey + `="""approve"""`},
		{"--config", sessionbusApprovalConfigKey + `=approve`},
		{"--config", sessionbusApprovalConfigKey + `="approve`},
	} {
		got, err := processArguments(arguments)
		if err != nil || !slices.Equal(got, arguments) {
			t.Fatalf("equivalent enabling arguments=%q got=%q err=%v", arguments, got, err)
		}
	}
	for _, arguments := range [][]string{
		{"-c", `features.plugins=true`},
		{"-c", sessionbusApprovalConfig},
	} {
		interactive, err := parseInteractiveOptions(arguments)
		if err != nil {
			t.Fatalf("interactive equivalent enable arguments=%q err=%v", arguments, err)
		}
		if got := append(ActivationArguments(), interactive.native...); !slices.Contains(got, sessionbusApprovalConfig) {
			t.Fatalf("interactive managed grant missing from %q", got)
		}
		lane, err := processArguments(arguments)
		if err != nil {
			t.Fatalf("lane equivalent enable arguments=%q err=%v", arguments, err)
		}
		if got := append(ActivationArguments(), lane...); !slices.Contains(got, sessionbusApprovalConfig) {
			t.Fatalf("lane managed grant missing from %q", got)
		}
	}
}

func TestManagedConfigCannotBeReplacedInInteractiveLaunch(t *testing.T) {
	for _, arguments := range [][]string{
		{"--config", `plugins.codex@sessionbus-peers.enabled=false`},
		{"--disable", "plugins"},
	} {
		if _, _, err := InteractivePlan(arguments, nil); err == nil || !strings.Contains(err.Error(), "managed Sessionbus grant") {
			t.Fatalf("interactive plan arguments=%q err=%v", arguments, err)
		}
		if _, err := parseInteractiveOptions(arguments); err == nil || !strings.Contains(err.Error(), "managed Sessionbus grant") {
			t.Fatalf("interactive launch arguments=%q parse err=%v", arguments, err)
		}
	}
	literal := []string{"--", "--config", sessionbusApprovalConfigKey + `="deny"`}
	if options, err := parseInteractiveOptions(literal); err != nil || !slices.Equal(options.native, literal) {
		t.Fatalf("post-boundary literal options=%#v err=%v", options, err)
	}
}

func TestManagedGrantCoexistsWithYoloAndNativeBypass(t *testing.T) {
	const bypass = "--dangerously-bypass-approvals-and-sandbox"
	for _, test := range []struct {
		arguments []string
		native    []string
	}{
		{[]string{"--model", "native", "--yolo", "-c", `model="caller"`}, []string{"--model", "native", bypass, "-c", `model="caller"`}},
		{[]string{"--model", "native", bypass, "-c", `model="caller"`}, []string{"--model", "native", bypass, "-c", `model="caller"`}},
	} {
		options, err := parseInteractiveOptions(test.arguments)
		if err != nil {
			t.Fatalf("interactive options arguments=%q err=%v", test.arguments, err)
		}
		got := append(ActivationArguments(), options.native...)
		want := append(ActivationArguments(), test.native...)
		if !slices.Equal(got, want) {
			t.Fatalf("managed launch arguments=%q want=%q", got, want)
		}
	}

	for _, selected := range []string{"--yolo", bypass} {
		lane, nativeBypass, err := laneArguments([]string{"--model", "native", selected, "-c", `model="caller"`})
		if err != nil {
			t.Fatal(err)
		}
		got := append(append([]string{"app-server", "--stdio"}, ActivationArguments()...), lane...)
		want := append(append([]string{"app-server", "--stdio"}, ActivationArguments()...), "--model", "native", "-c", `model="caller"`)
		if !nativeBypass || !slices.Equal(got, want) {
			t.Fatalf("managed lane selection=%q bypass=%t arguments=%q want=%q", selected, nativeBypass, got, want)
		}
		approval, sandbox, err := permission("", nativeBypass)
		if err != nil || approval != "never" || sandbox != "danger-full-access" {
			t.Fatalf("managed lane selection=%q policy=%q,%q err=%v", selected, approval, sandbox, err)
		}
	}
}

func TestLaneBypassPreservesValuesAndBoundary(t *testing.T) {
	const bypass = "--dangerously-bypass-approvals-and-sandbox"
	for _, test := range []struct {
		arguments []string
		want      []string
		bypass    bool
	}{
		{[]string{"--model", bypass, "--enable", "feature"}, []string{"--model", bypass, "--enable", "feature"}, false},
		{[]string{"--model=native", bypass, "--enable", "feature"}, []string{"--model=native", "--enable", "feature"}, true},
		{[]string{"--", bypass, "--yolo"}, []string{"--", bypass, "--yolo"}, false},
		{[]string{bypass + "=true"}, []string{bypass + "=true"}, false},
	} {
		got, selected, err := laneArguments(test.arguments)
		if err != nil || selected != test.bypass || !slices.Equal(got, test.want) {
			t.Fatalf("laneArguments(%q) = %q, %t, %v; want %q, %t", test.arguments, got, selected, err, test.want, test.bypass)
		}
	}
}

func TestLaneTypedArgumentConflicts(t *testing.T) {
	for _, test := range []struct {
		name, model, effort, want string
		arguments                 []string
	}{
		{name: "short model", model: "typed", arguments: []string{"-m", "native"}, want: "model"},
		{name: "long model", model: "typed", arguments: []string{"--model", "native"}, want: "model"},
		{name: "attached model", model: "typed", arguments: []string{"--model=native"}, want: "model"},
		{name: "short config model", model: "typed", arguments: []string{"-c", `model="native"`}, want: "model"},
		{name: "long config model", model: "typed", arguments: []string{"--config", `model="native"`}, want: "model"},
		{name: "attached long config model", model: "typed", arguments: []string{`--config=model="native"`}, want: "model"},
		{name: "attached short config model", model: "typed", arguments: []string{`-cmodel="native"`}, want: "model"},
		{name: "equals short config model", model: "typed", arguments: []string{`-c=model="native"`}, want: "model"},
		{name: "short config effort", effort: "low", arguments: []string{"-c", `model_reasoning_effort="high"`}, want: "reasoning_effort"},
		{name: "long config effort", effort: "low", arguments: []string{"--config", `model_reasoning_effort="high"`}, want: "reasoning_effort"},
		{name: "attached long config effort", effort: "low", arguments: []string{`--config=model_reasoning_effort="high"`}, want: "reasoning_effort"},
		{name: "attached short config effort", effort: "low", arguments: []string{`-cmodel_reasoning_effort="high"`}, want: "reasoning_effort"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateLaneTypedArguments(test.arguments, test.model, test.effort)
			if err == nil || err.Error() != "argument conflicts with typed field "+test.want {
				t.Fatalf("validateLaneTypedArguments(%q) error = %v", test.arguments, err)
			}
		})
	}
}

func TestLaneTypedArgumentConflictBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, model, effort string
		arguments           []string
	}{
		{name: "untyped model passthrough", arguments: []string{"--model", "native", "-c", `model="caller"`}},
		{name: "untyped effort passthrough", arguments: []string{"-c", `model_reasoning_effort="high"`}},
		{name: "post boundary model", model: "typed", effort: "low", arguments: []string{"--", "--model", "native", "-c", `model_reasoning_effort="high"`}},
		{name: "different config paths", model: "typed", effort: "low", arguments: []string{"-c", `model_provider="native"`, "--config", `model_reasoning_effort_extra="high"`}},
		{name: "literal quoted key", model: "typed", arguments: []string{"-c", `"model"="native"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateLaneTypedArguments(test.arguments, test.model, test.effort); err != nil {
				t.Fatalf("validateLaneTypedArguments(%q) error = %v", test.arguments, err)
			}
		})
	}
}

func TestPermissionAndName(t *testing.T) {
	for _, test := range []struct {
		input, approval, sandbox, failure string
		bypass                            bool
	}{
		{input: ""}, {input: "default"},
		{input: "bypassPermissions", approval: "never", sandbox: "danger-full-access"},
		{input: "", bypass: true, approval: "never", sandbox: "danger-full-access"},
		{input: "never", approval: "never"},
		{input: "never", bypass: true, approval: "never", sandbox: "danger-full-access"},
		{input: "on-request", approval: "on-request"},
		{input: "untrusted", approval: "untrusted"},
		{input: "on-request", bypass: true, failure: "permission_mode=on-request conflicts with " + codexNativeBypass},
	} {
		approval, sandbox, err := permission(test.input, test.bypass)
		if approval != test.approval || sandbox != test.sandbox || (err != nil && err.Error() != test.failure) {
			t.Fatalf("permission(%q, %t) = %q, %q, %v", test.input, test.bypass, approval, sandbox, err)
		}
		if test.failure != "" && err == nil {
			t.Fatalf("permission(%q, %t) unexpectedly succeeded", test.input, test.bypass)
		}
	}
	if got, err := namePart("parent/leaf@host"); err != nil || got != "parent/leaf" {
		t.Fatalf("name = %q, %v", got, err)
	}
}

func TestInteractivePlan(t *testing.T) {
	t.Setenv("CODEX_HOME", "/codex-home")
	plan, coordinated, err := InteractivePlan([]string{"--model", "-g", "--peer-name", "chosen", "resume", "id"}, []string{"PATH=/bin"})
	if err != nil {
		t.Fatal(err)
	}
	if !coordinated || !slices.Equal(plan.Args, []string{"--remote", "unix:///codex-home/app-server-control/app-server-control.sock", "--model", "-g", "resume", "id"}) {
		t.Fatalf("args = %v", plan.Args)
	}
	if !slices.Contains(plan.Env, host.GroupsEnv+`=[]`) || !slices.Contains(plan.Env, host.NameEnv+"=chosen") {
		t.Fatalf("env = %v", plan.Env)
	}
	if _, _, err = InteractivePlan([]string{"-g", "team"}, nil); err == nil || !strings.Contains(err.Error(), "reinstall with --codex-groups") {
		t.Fatalf("per-launch groups = %v", err)
	}
}

func TestInteractivePlanRemoteAndPassthrough(t *testing.T) {
	for _, argument := range []string{"--remote", "--remote=x", "--remote-auth-token-env"} {
		if _, _, err := InteractivePlan([]string{argument}, nil); err == nil || !strings.Contains(err.Error(), "caller-controlled --remote") {
			t.Fatalf("%s: %v", argument, err)
		}
	}
	for _, arguments := range [][]string{
		{"--help", "--peer-name="},
		{"exec", "-g"},
	} {
		environment := []string{"PATH=/bin"}
		plan, coordinated, err := InteractivePlan(arguments, environment)
		if err != nil || coordinated || !slices.Equal(plan.Args, arguments) || !slices.Equal(plan.Env, environment) {
			t.Fatalf("passthrough = %#v, %v", plan, err)
		}
	}
	for _, selector := range []string{"resume", "fork"} {
		plan, coordinated, err := InteractivePlan([]string{selector, "id"}, nil)
		if err != nil || !coordinated || !slices.Equal(plan.Args[2:], []string{selector, "id"}) {
			t.Fatalf("%s: %#v, %v", selector, plan, err)
		}
	}
	for _, argument := range []string{"-i", "--image=x", "--local-provider", "--add-dir=x"} {
		if _, coordinated, err := InteractivePlan([]string{argument, "value"}, nil); err != nil || !coordinated {
			t.Fatalf("%s: %v", argument, err)
		}
	}
}

func TestInteractiveDaemonStartScrubsBusEnvironment(t *testing.T) {
	t.Setenv("GO_WANT_CODEX_DAEMON", "1")
	evidence := t.TempDir() + "/daemon.json"
	t.Setenv("CODEX_TEST_EVIDENCE", evidence)
	t.Setenv(host.TokenEnv, "secret")
	original := peerDaemonCommand
	peerDaemonCommand = func(ctx context.Context, _ string, arguments ...string) *exec.Cmd {
		return exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=TestCodexDaemonProcess", "--"}, arguments...)...)
	}
	t.Cleanup(func() { peerDaemonCommand = original })
	if err := StartPeerDaemon(context.Background(), "codex"); err != nil {
		t.Fatal(err)
	}
	var observed struct{ Args, Env []string }
	body, err := os.ReadFile(evidence)
	if err != nil || json.Unmarshal(body, &observed) != nil || !slices.Equal(observed.Args, []string{"app-server", "daemon", "start"}) || slices.Contains(observed.Env, host.TokenEnv) {
		t.Fatalf("observed = %#v, %v", observed, err)
	}
}

func TestCodexDaemonProcess(t *testing.T) {
	if os.Getenv("GO_WANT_CODEX_DAEMON") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	names := []string{}
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		names = append(names, name)
	}
	body, _ := json.Marshal(map[string]any{"Args": os.Args[separator+1:], "Env": names})
	_ = os.WriteFile(os.Getenv("CODEX_TEST_EVIDENCE"), body, 0o600)
	os.Exit(0)
}
