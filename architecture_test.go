// SPDX-License-Identifier: MIT

package sessionbus_peers_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	peersModule             = "github.com/sessionbus/codex-peer"
	sdkModule               = "github.com/antst/sessionbus/bus/sdk/go"
	sdkVersion              = "v0.5.8-0.20260928234556-becb15b98893"
	citationCount           = 3
	citationReachabilitySHA = "b960d91495fc7df2830d441ffab924377e62dbbeeb9fece26c12b4f427e3fe4b"
	factsHeader             = "> Historical source note: citations to pre-split Sessionbus paths resolve in\n> the Forgejo `ai/sessionbus` repository through its `legacy-*` branches.\n> Citations to product source resolve in the external repository and full\n> commit recorded by the split archive manifest. Host evidence paths are\n> immutable external artifacts, not repository paths."
)

func TestRepositoryBoundary(t *testing.T) {
	allowed := map[string]bool{
		".forgejo": true, ".git": true,
		".github": true, ".gitignore": true,
		".golangci.yml": true, "LICENSE": true, "README.md": true,
		"RELEASE_VERSION":      true,
		"architecture_test.go": true, "version_test.go": true, "codex": true, "cmd": true,
		"docs": true, "go.mod": true, "go.sum": true,
		"scripts": true, "wrappers": true,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] {
			t.Errorf("path is outside the peers boundary: %s", entry.Name())
		}
	}
	for _, path := range []string{"internal", "wrappers/host", "wrappers/mcp", "scripts/cleanup-legacy", "bus", "integrations", "deploy", "examples", ".specify", ".agents", ".codex-plugin", "hooks", "skills", "go.work", "go.work.sum", "Makefile"} {
		if _, err := os.Stat(filepath.FromSlash(path)); !os.IsNotExist(err) {
			t.Errorf("legacy or cross-repository path remains: %s", path)
		}
	}
	wantCommands := []string{"codex-peer"}
	gotCommands := directoryNames(t, "cmd")
	if !equalStrings(gotCommands, wantCommands) {
		t.Errorf("peer command roots = %v, want %v", gotCommands, wantCommands)
	}
	if got := directoryNames(t, "wrappers"); !equalStrings(got, []string{"codex"}) {
		t.Errorf("wrapper roots = %v, want [codex]", got)
	}
	if _, err := os.Stat(".github/workflows/release.yml"); !os.IsNotExist(err) {
		t.Fatal("initial peers root must not contain a release workflow")
	}
}

func TestModuleAndImportBoundary(t *testing.T) {
	module := read(t, "go.mod")
	if !bytes.Contains(module, []byte("module "+peersModule+"\n")) || !bytes.Contains(module, []byte(sdkModule+" "+sdkVersion)) {
		t.Fatalf("go.mod violates the peers module shape:\n%s", module)
	}
	if !bytes.Contains(module, []byte("github.com/sessionbus/peer-common v0.0.0-20260928235110-ed8046c9ea51")) {
		t.Fatal("shared support must use the reviewed immutable module version")
	}
	if bytes.Contains(module, []byte("replace ")) {
		t.Fatal("peers go.mod contains a filesystem replacement")
	}
	for _, path := range []string{"go.work", "go.work.sum"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s must not be committed", path)
		}
	}
	checkGoImports(t, ".", func(path, imported string) {
		if strings.HasPrefix(imported, "github.com/antst/sessionbus/bus/internal/") {
			t.Errorf("%s imports daemon internal package %s", path, imported)
		}
		if strings.HasPrefix(imported, "github.com/antst/sessionbus/") && imported != sdkModule && !strings.HasPrefix(imported, sdkModule+"/") {
			t.Errorf("%s imports non-SDK Sessionbus package %s", path, imported)
		}
		if strings.HasPrefix(imported, "github.com/antst/sessionbus/wrappers/") {
			t.Errorf("%s retains pre-split wrapper import %s", path, imported)
		}
	})
	command := exec.Command("go", "list", "-m", "all")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("independent peers module graph: %v\n%s", err, output)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(output), []byte{'\n'}) {
		if bytes.Equal(line, []byte("github.com/antst/sessionbus")) {
			t.Fatalf("module graph contains the daemon root:\n%s", output)
		}
	}
}

func TestFactsHeadersAndReachabilityAudit(t *testing.T) {
	entries, err := os.ReadDir("docs/products")
	if err != nil {
		t.Fatal(err)
	}
	citationPattern := regexp.MustCompile("`([0-9a-f]{7,40}:[^`\\s]+)`")
	citations := make([]string, 0, citationCount)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".md" {
			continue
		}
		path := filepath.Join("docs/products", entry.Name())
		body := read(t, path)
		lines := bytes.Split(body, []byte{'\n'})
		if len(lines) < 8 || len(lines[0]) == 0 || !bytes.Equal(bytes.Join(lines[2:7], []byte{'\n'}), []byte(factsHeader)) {
			t.Errorf("facts header is absent or misplaced: %s", path)
		}
		for _, match := range citationPattern.FindAllSubmatch(body, -1) {
			citations = append(citations, string(match[1]))
		}
	}
	sort.Strings(citations)
	digest := sha256.Sum256([]byte(strings.Join(citations, "\n") + "\n"))
	if len(citations) != citationCount || hex.EncodeToString(digest[:]) != citationReachabilitySHA {
		t.Fatalf("historical citations differ from the resolved split archive audit: count=%d sha256=%s", len(citations), hex.EncodeToString(digest[:]))
	}
}

func TestFormerBrandGuardForProductFacts(t *testing.T) {
	err := filepath.WalkDir("docs/products", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".md" {
			return err
		}
		cleaned, cleanErr := removeHistoricalExceptions(read(t, path))
		if cleanErr != nil {
			t.Errorf("%s: %v", path, cleanErr)
			return nil
		}
		if containsFormerBrand(cleaned) {
			t.Errorf("former brand remains outside a fenced capture, evidence path, or commit-qualified citation: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSPDXAndLicenseCoverage(t *testing.T) {
	if !bytes.Contains(read(t, "LICENSE"), []byte("MIT License")) {
		t.Fatal("root MIT license is missing")
	}
	if err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if ignoredDirectory(path) {
				return filepath.SkipDir
			}
			return nil
		}
		body := read(t, path)
		if bytes.Contains(body, []byte("SPDX-License-Identifier: "+"GPL")) {
			t.Errorf("GPL SPDX identifier remains in peers tree: %s", path)
		}
		if filepath.Ext(path) == ".go" && firstOrSecondLine(body) != "// SPDX-License-Identifier: MIT" {
			t.Errorf("Go source lacks MIT SPDX header: %s", path)
		}
		if filepath.Ext(path) == ".mjs" && firstOrSecondLine(body) != "// SPDX-License-Identifier: MIT" {
			t.Errorf("JavaScript source lacks MIT SPDX header: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"docs/designs/codex-0.5.0/legacy-lane-skill/scripts/lane-preflight",
		"scripts/codex-mcp", "scripts/test-codex-mcp",
	} {
		if firstOrSecondLine(read(t, path)) != "# SPDX-License-Identifier: MIT" {
			t.Errorf("shell source lacks MIT SPDX header: %s", path)
		}
	}
}

func TestRepositoryURLsAndRemovedPaths(t *testing.T) {
	for _, root := range []string{"codex", "scripts", "wrappers/README.md"} {
		if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if ignoredDirectory(path) {
					return filepath.SkipDir
				}
				return nil
			}
			body := read(t, path)
			if bytes.Contains(body, []byte("github.com/antst/sessionbus.git")) || bytes.Contains(body, []byte("integrations/opencode")) {
				t.Errorf("active peer asset points to pre-split repository path: %s", path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadmeIsTheSourceInstallAuthority(t *testing.T) {
	readme := read(t, "README.md")
	for _, exact := range []string{"scripts/package-codex ./dist", "codex/README.md", "scripts/install-codex.sh", "github.com/sessionbus/codex-peer", "docs/migration/FUNCTIONALITY-CHECKLIST.md"} {
		if !bytes.Contains(readme, []byte(exact)) {
			t.Errorf("README lacks %q", exact)
		}
	}
}

func checkGoImports(t *testing.T, root string, check func(string, string)) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if ignoredDirectory(path) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, specification := range file.Imports {
			imported, err := strconv.Unquote(specification.Path.Value)
			if err != nil {
				return err
			}
			check(path, imported)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func ignoredDirectory(path string) bool {
	base := filepath.Base(path)
	return base == ".git" || base == "node_modules" || base == "dist" || base == "bin"
}

func removeHistoricalExceptions(body []byte) ([]byte, error) {
	evidence := regexp.MustCompile(`/home/antst/agentbus-evidence/[^\x60\s]+`)
	citation := regexp.MustCompile("`[0-9a-f]{7,40}:[^`]+`")
	cleaned := make([]byte, 0, len(body))
	fenced := false
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if bytes.HasPrefix(bytes.TrimSpace(line), []byte("```")) {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		line = evidence.ReplaceAll(line, nil)
		line = citation.ReplaceAll(line, nil)
		cleaned = append(cleaned, line...)
		cleaned = append(cleaned, '\n')
	}
	if fenced {
		return nil, os.ErrInvalid
	}
	return cleaned, nil
}

func containsFormerBrand(body []byte) bool {
	lower := bytes.ToLower(body)
	for _, former := range [][]byte{
		[]byte("agent" + "bus"), []byte("agent" + "_sessions"),
		[]byte("agent" + "-sessions"),
	} {
		if bytes.Contains(lower, former) {
			return true
		}
	}
	words := bytes.Join(bytes.Fields(body), []byte{' '})
	return bytes.Contains(words, []byte("Agent "+"Sessions"))
}

func directoryNames(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func firstOrSecondLine(body []byte) string {
	lines := bytes.SplitN(body, []byte{'\n'}, 3)
	if len(lines) > 0 && bytes.HasPrefix(lines[0], []byte("#!")) && len(lines) > 1 {
		return string(lines[1])
	}
	if len(lines) > 0 {
		return string(lines[0])
	}
	return ""
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
