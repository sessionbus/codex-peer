// SPDX-License-Identifier: MIT

package release_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureRevision = "abcdef0123456789abcdef0123456789abcdef01"

func TestVersionGuardAcceptsDevelopmentAndMatchingStableTag(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, release := range []string{"development", "v0.5.9"} {
		t.Run(release, func(t *testing.T) {
			output, err := runVersionGuard(root, release)
			if err != nil {
				t.Fatalf("guard: %v\n%s", err, output)
			}
			want := "0.5.9|" + release + "|" + fixtureRevision + "\n"
			if string(output) != want {
				t.Fatalf("output=%q want=%q", output, want)
			}
		})
	}
}

func TestVersionGuardRejectsEveryStableVersionAuthorityMismatch(t *testing.T) {
	tests := []struct {
		name, releaseVersion, claudeVersion, codexVersion, release, errorText string
	}{
		{"tag", "0.5.2", "0.5.2", "0.5.2-codex.0", "v0.5.3", "Stable peer release v0.5.3 does not match RELEASE_VERSION v0.5.2"},
		{"codex", "0.5.2", "0.5.2", "0.5.1-codex.0", "v0.5.2", "Codex base version 0.5.1-codex.0 does not match RELEASE_VERSION 0.5.2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeVersionFixture(t, root, test.releaseVersion, test.claudeVersion, test.codexVersion)
			output, err := runVersionGuard(root, test.release)
			if err == nil || !strings.Contains(string(output), test.errorText) {
				t.Fatalf("err=%v output=%q", err, output)
			}
		})
	}
}

func runVersionGuard(root, release string) ([]byte, error) {
	script, err := filepath.Abs("version.sh")
	if err != nil {
		return nil, err
	}
	command := exec.Command("sh", "-c", `set -eu
. "$1"
peer_version_init "$2"
printf '%s|%s|%s\n' "$peer_base_version" "$peer_release" "$peer_revision"`, "fixture", script, root)
	command.Env = append(os.Environ(), "SESSIONBUS_PEERS_RELEASE="+release, "SESSIONBUS_PEERS_REVISION="+fixtureRevision)
	return command.CombinedOutput()
}

func writeVersionFixture(t *testing.T, root, releaseVersion, claudeVersion, codexVersion string) {
	t.Helper()
	files := map[string]string{
		"RELEASE_VERSION":                                   releaseVersion + "\n",
		"claude/.claude-plugin/plugin.json":                 `{"version":"` + claudeVersion + `"}`,
		"codex/marketplace/codex/.codex-plugin/plugin.json": `{"version":"` + codexVersion + `"}`,
	}
	for path, body := range files {
		path = filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
