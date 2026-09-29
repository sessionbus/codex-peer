// SPDX-License-Identifier: MIT
package codex

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestCodexArchiveContainsOneArtifactAndOneGenericSkill(t *testing.T) {
	root := filepath.Join("..", "..")
	output := filepath.Join(t.TempDir(), "archive output")
	cmd := exec.Command("sh", "scripts/package-codex", output)
	cmd.Dir = root
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pack: %v\n%s", err, raw)
	}
	file, err := os.Open(filepath.Join(output, "codex-peer-"+runtime.GOOS+"-"+runtime.GOARCH+".tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	var regular []string
	aliases := map[string]string{}
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch h.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeSymlink:
			aliases[h.Name] = h.Linkname
		case tar.TypeReg:
			regular = append(regular, h.Name)
			if h.Name == "marketplace/codex/.codex-plugin/plugin.json" {
				var m struct{ Name, Version string }
				if json.NewDecoder(reader).Decode(&m) != nil || m.Name != "codex" || !strings.HasPrefix(m.Version, "0.5.9-codex.g") {
					t.Fatalf("manifest=%+v", m)
				}
			} else if h.Name == "SOURCE.txt" {
				body, err := io.ReadAll(reader)
				if err != nil || len(strings.TrimSpace(string(body))) != 40 {
					t.Fatalf("SOURCE.txt=%q err=%v", body, err)
				}
			}
		default:
			t.Fatalf("unexpected tar member type %d: %s", h.Typeflag, h.Name)
		}
	}
	sort.Strings(regular)
	want := []string{"LICENSE", "ROLE", "SOURCE.txt", "THIRD-PARTY-NOTICES.txt", "README.md", "bin/codex-peer", "install", "marketplace/.agents/plugins/marketplace.json", "marketplace/codex/.codex-plugin/plugin.json", "marketplace/codex/.mcp.json", "marketplace/codex/skills/sessionbus/SKILL.md", "uninstall"}
	sort.Strings(want)
	if !reflect.DeepEqual(regular, want) {
		t.Fatalf("regular payload=%q", regular)
	}
	wantAliases := map[string]string{"bin/" + MCPAlias: "codex-peer", "bin/" + BrokerAlias: "codex-peer", "bin/" + InstallAlias: "codex-peer"}
	if !reflect.DeepEqual(aliases, wantAliases) {
		t.Fatalf("private aliases=%v", aliases)
	}
}

func TestInstallReplacesOwnedMarketplaceWithoutStaleSkills(t *testing.T) {
	// Execute the literal shell recipe using one compiled stand-in. No Codex,
	// service, model or real user configuration runs in this offline fixture.
	fixture := t.TempDir()
	home := filepath.Join(fixture, "fixture home")
	payload := filepath.Join(fixture, "archive payload")
	if err := os.MkdirAll(filepath.Join(payload, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(fixture, "main.go")
	code := `package main
import("os";"path/filepath")
func main(){if filepath.Base(os.Args[0])!="codex-peer-install"||len(os.Args)!=2{os.Exit(4)};if err:=os.WriteFile(filepath.Join(filepath.Dir(filepath.Dir(os.Args[0])),"fixture-registration"),[]byte(os.Args[1]),0600);err!=nil{panic(err)}}`
	if err := os.WriteFile(source, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", filepath.Join(payload, "bin", "codex-peer"), source)
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("stand-in: %v %s", err, raw)
	}
	for _, name := range []string{"install", "uninstall", "README.md", "THIRD-PARTY-NOTICES.txt"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "codex", name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(payload, name), body, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(payload, "LICENSE"), []byte("fixture license"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(payload, "SOURCE.txt"), []byte(strings.Repeat("a", 40)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wanted := filepath.Join(payload, "marketplace", "codex", "skills", "sessionbus", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(wanted), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wanted, []byte("current generic skill"), 0600); err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(home, ".local", "share", "sessionbus", "codex", "marketplace")
	stale := filepath.Join(installed, "codex", "skills", "obsolete", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(stale), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("obsolete"), 0600); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(home, ".local", "share", "sessionbus", "other-product")
	if err := os.WriteFile(unrelated, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", filepath.Join(payload, "install"))
	// Satisfy the native prerequisite without executing a native product in CI.
	if err := os.Symlink(filepath.Join(payload, "bin", "codex-peer"), filepath.Join(payload, "bin", "codex")); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "HOME="+home, "PATH="+filepath.Join(payload, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("install recipe: %v %s", err, raw)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale active skill survived: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(installed, "codex", "skills", "sessionbus", "SKILL.md")); err != nil || string(b) != "current generic skill" {
		t.Fatalf("current skill=%q %v", b, err)
	}
	if b, err := os.ReadFile(unrelated); err != nil || string(b) != "untouched" {
		t.Fatalf("unrelated path=%q %v", b, err)
	}
	packagePath := filepath.Join(home, ".local", "libexec", "sessionbus", "codex")
	if b, err := os.ReadFile(filepath.Join(packagePath, "fixture-registration")); err != nil || string(b) != installed {
		t.Fatalf("private installer dispatch=%q %v", b, err)
	}
	public, err := filepath.EvalSymlinks(filepath.Join(home, ".local", "bin", "codex-peer"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := filepath.EvalSymlinks(filepath.Join(packagePath, "bin", "codex-peer"))
	if err != nil || public != expected {
		t.Fatalf("public bin=%q %v", public, err)
	}
	if b, err := os.ReadFile(filepath.Join(packagePath, "SOURCE.txt")); err != nil || string(b) != strings.Repeat("a", 40)+"\n" {
		t.Fatalf("installed source=%q %v", b, err)
	}
}
