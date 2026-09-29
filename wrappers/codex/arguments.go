// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

var peerDaemonCommand = exec.CommandContext

const codexNativeBypass = "--dangerously-bypass-approvals-and-sandbox"

// These are native App Server arguments, validated by Codex itself.
func processArguments(arguments []string) ([]string, error) {
	if err := validateManagedConfig(arguments); err != nil {
		return nil, err
	}
	return append([]string(nil), arguments...), nil
}

// validateLaneTypedArguments prevents a raw native setting from competing with
// the corresponding typed App Server field. Untyped native arguments remain
// native-owned, and operands after -- are always literal.
func validateLaneTypedArguments(arguments []string, model, reasoningEffort string) error {
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			return nil
		}
		if value, found := codexConfigValue(arguments, &index); found {
			path, _, ok := codexConfigAssignment(value)
			if !ok {
				continue
			}
			if model != "" && slices.Equal(path, []string{"model"}) {
				return errors.New("argument conflicts with typed field model")
			}
			if reasoningEffort != "" && slices.Equal(path, []string{"model_reasoning_effort"}) {
				return errors.New("argument conflicts with typed field reasoning_effort")
			}
			continue
		}
		if model != "" && (argument == "-m" || argument == "--model" || strings.HasPrefix(argument, "--model=")) {
			return errors.New("argument conflicts with typed field model")
		}
	}
	return nil
}

// laneArguments consumes the TUI-only bypass spelling before starting App
// Server. Earlier lane dispatch canonicalized this flag to permission_mode;
// App Server instead accepts the equivalent policy through thread and turn
// requests. Known scalar option values and post-- operands retain their bytes.
func laneArguments(arguments []string) ([]string, bool, error) {
	arguments, err := processArguments(arguments)
	if err != nil {
		return nil, false, err
	}
	native := make([]string, 0, len(arguments))
	bypass := false
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			native = append(native, arguments[index:]...)
			break
		}
		if argument == codexNativeBypass || argument == "--yolo" {
			bypass = true
			continue
		}
		native = append(native, argument)
		key, _, attached := strings.Cut(argument, "=")
		if !attached && codexOptionTakesValue(key) && index+1 < len(arguments) && arguments[index+1] != "--" {
			index++
			native = append(native, arguments[index])
		}
	}
	return native, bypass, nil
}

var managedConfigPaths = [][]string{
	{"features", "plugins"},
	{"plugins", PluginID, "enabled"},
	{"plugins", PluginID, "mcp_servers", "sessionbus", "enabled"},
	{"plugins", PluginID, "mcp_servers", "sessionbus", "enabled_tools"},
	{"plugins", PluginID, "mcp_servers", "sessionbus", "disabled_tools"},
	{"plugins", PluginID, "mcp_servers", "sessionbus", "tools", "sessionbus", "approval_mode"},
}

// validateManagedConfig rejects only arguments that can disable or replace the
// managed plugin or its Sessionbus tool. Native CLI config keys split on dots
// without unquoting; only their values use TOML syntax.
func validateManagedConfig(arguments []string) error {
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			return nil
		}
		value, found := codexConfigValue(arguments, &index)
		switch {
		case argument == "--disable":
			if index+1 < len(arguments) && arguments[index+1] != "--" {
				index++
				if arguments[index] == "plugins" {
					return errors.New("disabling the plugins feature conflicts with the managed Sessionbus grant")
				}
			}
		case strings.HasPrefix(argument, "--disable="):
			if strings.TrimPrefix(argument, "--disable=") == "plugins" {
				return errors.New("disabling the plugins feature conflicts with the managed Sessionbus grant")
			}
		}
		if !found {
			continue
		}
		path, configValue, ok := codexConfigAssignment(value)
		if !ok {
			continue // Native Codex owns malformed and non-assignment config values.
		}
		for _, managed := range managedConfigPaths {
			if pathPrefix(path, managed) || pathPrefix(managed, path) {
				if slices.Equal(path, managed) && compatibleManagedConfig(path, configValue) {
					break
				}
				return fmt.Errorf("configuration %q conflicts with the managed Sessionbus grant", strings.Join(path, "."))
			}
		}
	}
	return nil
}

// codexConfigValue recognizes the exact config spellings already accepted by
// the wrapper. It advances index when a separate value is consumed.
func codexConfigValue(arguments []string, index *int) (string, bool) {
	argument := arguments[*index]
	switch {
	case argument == "-c" || argument == "--config":
		if *index+1 < len(arguments) && arguments[*index+1] != "--" && !strings.HasPrefix(arguments[*index+1], "-") {
			(*index)++
			return arguments[*index], true
		}
	case strings.HasPrefix(argument, "--config="):
		return strings.TrimPrefix(argument, "--config="), true
	case strings.HasPrefix(argument, "-c") && len(argument) > len("-c"):
		value := strings.TrimPrefix(argument, "-c")
		return strings.TrimPrefix(value, "="), true
	}
	return "", false
}

func pathPrefix(path, target []string) bool {
	if len(path) > len(target) {
		return false
	}
	for index := range path {
		if path[index] != target[index] {
			return false
		}
	}
	return true
}

// codexConfigAssignment mirrors native CLI override parsing: split once on
// '=', trim the whole key, and split it literally on dots. Quote bytes remain
// key bytes; the value remains TOML source for the narrow compatibility checks.
func codexConfigAssignment(value string) ([]string, string, bool) {
	key, configValue, found := strings.Cut(value, "=")
	key = strings.TrimSpace(key)
	if !found || key == "" {
		return nil, "", false
	}
	return strings.Split(key, "."), strings.TrimSpace(configValue), true
}

// compatibleManagedConfig preserves previously valid callers that repeat an
// enabling setting. Parent table assignments still conflict because they can
// erase the fixed leaf written earlier on the native command line.
func compatibleManagedConfig(path []string, value string) bool {
	switch strings.Join(path, ".") {
	case "features.plugins", "plugins." + PluginID + ".enabled", "plugins." + PluginID + ".mcp_servers.sessionbus.enabled":
		enabled, ok := codexTOMLBool(value)
		return ok && enabled
	case "plugins." + PluginID + ".mcp_servers.sessionbus.enabled_tools":
		tools, ok := tomlStringArray(value)
		return ok && slices.Contains(tools, "sessionbus")
	case "plugins." + PluginID + ".mcp_servers.sessionbus.disabled_tools":
		tools, ok := tomlStringArray(value)
		return ok && !slices.Contains(tools, "sessionbus")
	case sessionbusApprovalConfigKey:
		return codexTOMLString(value) == "approve"
	}
	return false
}

// tomlStringArray uses the same wrapped TOML shape as native Codex. Invalid
// TOML falls back to a raw string natively, which cannot satisfy a list field.
func tomlStringArray(value string) ([]string, bool) {
	var document struct {
		Value []string `toml:"_x_"`
	}
	_, err := toml.Decode("_x_ = "+value, &document)
	return document.Value, err == nil
}

func codexTOMLBool(value string) (bool, bool) {
	var document struct {
		Value bool `toml:"_x_"`
	}
	_, err := toml.Decode("_x_ = "+value, &document)
	return document.Value, err == nil
}

func codexTOMLString(value string) string {
	var document struct {
		Value string `toml:"_x_"`
	}
	if _, err := toml.Decode("_x_ = "+value, &document); err == nil {
		return document.Value
	}
	// Native Codex falls back to a trimmed raw string when TOML parsing fails.
	return strings.Trim(strings.TrimSpace(value), `"'`)
}
func permission(value string, nativeBypass bool) (string, string, error) {
	switch value {
	case "", "default":
		if nativeBypass {
			return "never", "danger-full-access", nil
		}
		return "", "", nil
	case "bypassPermissions":
		return "never", "danger-full-access", nil
	case "never":
		if nativeBypass {
			return "never", "danger-full-access", nil
		}
		return "never", "", nil
	default:
		if nativeBypass {
			return "", "", fmt.Errorf("permission_mode=%s conflicts with %s", value, codexNativeBypass)
		}
		return value, "", nil
	}
}

func namePart(name string) (string, error) {
	index := strings.LastIndexByte(name, '@')
	if index < 1 {
		return "", errors.New("Codex lane name is invalid")
	}
	return name[:index], nil
}

func InteractivePlan(arguments, environment []string) (host.ExecPlan, bool, error) {
	remote := false
	plan, passthrough, err := host.ClassifiedInteractivePlan("codex", arguments, environment, host.PeerIdentity{}, func(argument string) bool {
		key, _, attached := strings.Cut(argument, "=")
		if key == "--remote" || key == "--remote-auth-token-env" {
			remote = true
		}
		return !attached && codexOptionTakesValue(key)
	}, func(argument string) bool {
		return argument == "-h" || argument == "--help" || argument == "-V" || argument == "--version" || codexSubcommand(argument)
	})
	if err != nil {
		return host.ExecPlan{}, false, err
	}
	if passthrough {
		return plan, false, nil
	}
	if err := validateManagedConfig(plan.Args); err != nil {
		return host.ExecPlan{}, false, err
	}
	if slices.ContainsFunc(plan.Env, func(value string) bool {
		return strings.HasPrefix(value, host.GroupsEnv+"=") && value != host.GroupsEnv+"=[]"
	}) {
		return host.ExecPlan{}, false, errors.New("Codex peer groups are configured by the installed sessionbus MCP entry; reinstall with --codex-groups")
	}
	if remote {
		return host.ExecPlan{}, false, errors.New("caller-controlled --remote options are not supported")
	}
	if !slices.ContainsFunc(plan.Env, func(value string) bool { return strings.HasPrefix(value, host.SocketEnv+"=") }) {
		plan.Env = append(plan.Env, host.SocketEnv+"="+sessionkit.Socket())
	}
	socket, err := appServerSocket()
	if err != nil {
		return host.ExecPlan{}, false, err
	}
	plan.Args = append([]string{"--remote", "unix://" + socket}, plan.Args...)
	return plan, true, nil
}

func appServerSocket() (string, error) {
	home := strings.TrimSpace(os.Getenv("CODEX_HOME"))
	if home == "" {
		user, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		home = filepath.Join(user, ".codex")
	}
	return filepath.Join(home, "app-server-control", "app-server-control.sock"), nil
}

func StartPeerDaemon(ctx context.Context, path string) error {
	command := peerDaemonCommand(ctx, path, "app-server", "daemon", "start")
	command.Env = slices.DeleteFunc(os.Environ(), func(value string) bool {
		key, _, _ := strings.Cut(value, "=")
		return strings.HasPrefix(key, "SESSIONBUS_")
	})
	if err := command.Run(); err != nil {
		return fmt.Errorf("start Codex App Server: %w", err)
	}
	return nil
}

func codexSubcommand(argument string) bool {
	switch argument {
	case "agents", "exec", "e", "review", "login", "logout", "mcp", "plugin", "mcp-server", "app-server", "remote-control", "completion", "update", "doctor", "sandbox", "debug", "apply", "a", "queue", "archive", "delete", "unarchive", "cloud", "app", "exec-server", "features", "help", "migrate-rollouts":
		return true
	}
	return false
}

func codexOptionTakesValue(name string) bool {
	_, found := map[string]bool{
		"-a": true, "--ask-for-approval": true, "-c": true, "--config": true,
		"-C": true, "--cd": true, "--disable": true, "--enable": true,
		"-i": true, "--image": true, "-m": true, "--model": true,
		"--local-provider": true, "-p": true, "--profile": true,
		"--remote": true, "--remote-auth-token-env": true,
		"-s": true, "--sandbox": true, "--add-dir": true,
	}[name]
	return found
}
