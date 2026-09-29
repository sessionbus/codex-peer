# Sessionbus Codex peer

Connect native Codex sessions through [Sessionbus](https://github.com/sessionbus/sessionbus).
`codex-peer` supports interactive sessions and managed lanes using the same
permanent installation, native history and native permission controls.

This is the original `sessionbus-peers` repository, renamed for Codex. Existing
links, releases, issues and history remain here. The other products now live in
[claude-peer](https://github.com/sessionbus/claude-peer),
[grok-peer](https://github.com/sessionbus/grok-peer),
[qwen-peer](https://github.com/sessionbus/qwen-peer),
[opencode-kilo](https://github.com/sessionbus/opencode-kilo), and
[pi-omp](https://github.com/sessionbus/pi-omp).
The [DSH plugin](https://github.com/sessionbus/sessionbus-dsh) remains separate.
Historical multi-product releases retain their original assets and scope.

Separation verification is tracked in the
[functionality checklist](docs/migration/FUNCTIONALITY-CHECKLIST.md).
A historical pass is not a claim that the separated installation has passed.

## Install

Install native Codex and [Sessionbus](https://github.com/sessionbus/sessionbus)
first, using your normal home, login, configuration and PATH. Then:

```sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/codex-peer/main/scripts/install-codex.sh | sh
```

The installer downloads the latest stable Codex archive and checks SHA256SUMS.
It selects the development prerelease only when no stable release exists;
lookup, checksum and archive errors stop installation. To pin a published tag,
set `SESSIONBUS_VERSION=vX.Y.Z` on `sh`; `SESSIONBUS_DOWNLOAD_ROOT` supports an
explicit mirror. Rerun the installer to update.

Linux and macOS, amd64 and arm64 are supported. The archive needs no Go, Node.js
or npm runtime on the target. It includes the executable, private aliases,
marketplace, generic skill, installer and uninstall script. Building the Go
command alone does not install the native plugin.

See [the Codex guide](codex/README.md) for the complete native installation,
permissions, lifecycle, resume, cleanup and uninstall contract. The installed
plugin ID `codex@sessionbus-peers` is retained for configuration compatibility;
the repository rename does not rename that native identifier. Ordinary `codex`
remains ordinary; managed launches activate Sessionbus explicitly.

## Build and test

Go 1.24 or later is required for development:

```sh
git clone https://github.com/sessionbus/codex-peer.git
cd codex-peer
GOWORK=off go mod download
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
scripts/test-codex-mcp
scripts/package-codex ./dist
```

Extract the resulting archive and run its `install` script in the real user
home. The package records its source in SOURCE.txt. `codex-peer --version` and
`-V` report the wrapper release/revision without starting native Codex;
`--native-version` requests the native version outside lane mode.

The stable tag, RELEASE_VERSION and Codex plugin base version must agree.
CI retains Linux/macOS tests, lint, workflow checks and four-platform packaging.
Stable releases are published from signed `vX.Y.Z` tags; see [docs/releases](docs/releases).

Shared host/MCP/version support comes from
[`peer-common`](https://github.com/sessionbus/peer-common) at the exact version
pinned in `go.mod` and `go.sum`. Updates require explicit review and regression
checks; a common release does not automatically change this peer.
The released Bus SDK keeps its existing Go module identity; the GitHub transfer
is not a reason to rewrite a dependency's module path.

## Delivery and presence


A delivery reported as `rejected` with reason `no_receipt` means that no usable
receipt was obtained. It does not prove the message was never submitted or
consumed, including when the recipient disconnects. Preserve the delivery ID,
reason and any run reference; report the uncertainty without automatically
resending. A later connected or idle-looking row does not make replay safe.

In a `list` row, `connected` describes the Sessionbus attachment and `running`
describes a daemon-managed Run. An interactive peer's `running:false` does not
prove its native model is idle. Do not use these flags to predict delivery
admission; follow the actual receipt.

The orchestrator's installed tool declaration governs the tool identifier and
`{action, arguments}` envelope. After selecting a lane product, that product's
`describe` response and product skill/README govern its open fields, native
permission values, delivery receipts and lifecycle. `describe` lists available
fields; product documentation supplies their meaning and allowed native values.
Do not apply the orchestrator product's native options to a different lane product.

### Parent tracing

The unified tool's `trace` action can enable `events` or `content` tracing for
one live direct child, and `spawn` accepts the same initial `trace` setting. The
default is `off`. The daemon sends at most one ordinary message copy to the
eligible parent after the original Sessionbus send settles; `content` includes
the body, while `events` contains message and delivery metadata. Run lifecycle,
native prompts, and native results are outside this initial scope.

Tracing adds no history, durable policy, replay, catch-up, or separate event
transport. It requires Sessionbus v0.5.4 or later on the daemon and every involved hub,
and updated peer tools. Upgrade all of them before requesting tracing across
hosts. See the [v0.5.1 notes](docs/releases/v0.5.1.md) and the
[Sessionbus communication-trace contract](https://github.com/sessionbus/sessionbus/blob/main/docs/designs/COMMUNICATION-TRACE.md)
for the complete authority and delivery rules.

The tracing relationship ends with the parent's live lifetime, even for a
persistent child. Reconnecting with an old parent ID does not recover that
relationship or replay copies. Older daemons reject the new `trace` action or
spawn field; a trace-aware daemon returns `unsupported_trace` when an involved
federation link cannot enforce the requested tracing controls.

## Compatibility and retained knowledge

Native argument order and literal `--` are preserved. `--resume` selects native
history; `--yolo` maps to Codex's native bypass flag. Default launches retain
native approval/sandbox policy while granting only the managed Sessionbus tool.
Caller attempts to disable that tool fail before startup. Profiles remain
TUI-only where the native remote protocol does not project them.

[Product facts](docs/products/codex.md),
[prior installed acceptance](docs/designs/codex-0.5.0/ACCEPTANCE.md), and
[mandatory wake boundaries](docs/designs/mandatory-message-wake-20260921/NATIVE-BOUNDARIES.md)
retain historical evidence and limitations. Earlier stage-only observations do
not override the current mandatory-wake contract. Legacy cleanup
fixtures are retained as historical compatibility tools, not the current install route.

## Links published before the split

The original `antst/sessionbus-peers` repository redirects here and retains its
history, issues and combined release assets (v0.5.0–v0.5.3 and `development`),
which remain available for explicit manual download. Its per-product
`scripts/install-<product>.sh` entrypoints are retired: the combined v0.5.3 peers
are incompatible with Sessionbus v0.5.8 and later. Install each product from its
own repository:

- Claude: [claude-peer `scripts/install-claude.sh`](https://github.com/sessionbus/claude-peer/blob/main/scripts/install-claude.sh)
- Grok: [grok-peer `scripts/install-grok.sh`](https://github.com/sessionbus/grok-peer/blob/main/scripts/install-grok.sh)
- Qwen: [qwen-peer `scripts/install-qwen.sh`](https://github.com/sessionbus/qwen-peer/blob/main/scripts/install-qwen.sh)
- OpenCode and Kilo: [opencode-kilo `scripts/install-opencode.sh`](https://github.com/sessionbus/opencode-kilo/blob/main/scripts/install-opencode.sh), [`scripts/install-kilo.sh`](https://github.com/sessionbus/opencode-kilo/blob/main/scripts/install-kilo.sh)
- Pi and OMP: [pi-omp `scripts/install-pi.sh`](https://github.com/sessionbus/pi-omp/blob/main/scripts/install-pi.sh), [`scripts/install-omp.sh`](https://github.com/sessionbus/pi-omp/blob/main/scripts/install-omp.sh)
