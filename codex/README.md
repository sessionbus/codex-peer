# Codex with Sessionbus

This Go package supports interactive Codex and daemon-owned Codex lanes through
one executable and one native plugin. Installed checks cover native identity,
public tools, parent-to-lane work, delivery, resume and process cleanup. The
acceptance ledger distinguishes those observations from controlled race tests.

## Install from an archive

Install the binary and native plugin in one step:

```sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/codex-peer/main/scripts/install-codex.sh | sh
```

The default is the latest stable release, falling back to development only
while no stable release exists. Set `SESSIONBUS_VERSION` on `sh` to select a
published release tag explicitly. The installer verifies its archive checksum
and uses the permanent native installation below.

Codex and the Sessionbus service must already be available in the real login
environment. Build an archive on a development host:

```sh
scripts/package-codex ./dist
```

Copy the matching OS/architecture archive to the target host, extract it, and
run its installer as your ordinary user:

```sh
mkdir codex-peer-package
tar -xzf codex-peer-linux-amd64.tar.gz -C codex-peer-package
./codex-peer-package/install
```

The installer replaces `~/.local/libexec/sessionbus/codex/bin/codex-peer` and
links `~/.local/bin/codex-peer` to it. Private MCP, broker and installer aliases
refer to that same compiled artifact. Put `~/.local/bin` on your login PATH if it
is not already there. The target host needs no Node.js, npm or Go runtime.

The owned marketplace lives at `~/.local/share/sessionbus/codex/marketplace`.
The installer uses native commands:

```sh
codex plugin marketplace add "$HOME/.local/share/sessionbus/codex/marketplace"
codex plugin add codex@sessionbus-peers --json
```

The marketplace command registers its own `marketplaces.sessionbus-peers`
source entry in native user configuration. The plugin add records native
`installedPath`, then the installer uses a zero-input native App Server
connection for the single `config/value/write` request:

```json
{"keyPath":"plugins.\"codex@sessionbus-peers\".enabled","value":false,"mergeStrategy":"replace"}
```

No `filePath` override is supplied: Codex edits its real user configuration,
following native symlink handling and preserving unrelated settings. The native
add initially enables the plugin; installation is complete only after the
following disable returns `ok`. A failure is reported as installation failure.
The package record is `~/.local/libexec/sessionbus/codex/installed.json`.
The rendered MCP manifest uses the absolute private binary path and explicitly
passes only the launch metadata it needs through native `env_vars`.

Plain `codex` uses ordinary native configuration with this plugin disabled.
A managed launch prefixes `-c features.plugins=true` and
`-c plugins.codex@sessionbus-peers.enabled=true` to enable the plugin. CLI keys
split on dots without unquoting segments; the quotes used by the separate
config-write API must not be embedded in this CLI key. The launcher
keeps the caller's native arguments in order, except that a caller cannot
disable or replace the managed plugin and its Sessionbus tool.
There is one generic `sessionbus` skill and public tool, shared across products.

Repeat the same archive install to update the permanent installation. Reinstall
replaces the owned marketplace payload, so removed skills do not remain active. The
archive includes a source-revision plugin version so native add sees the new
payload. Installation does not change the Sessionbus service or other plugins.

## Lane behavior

Use the generic Sessionbus tool's `describe` action for `codex-peer`, then
`spawn`, `start` or `run`, `status`/`wait`, `ack` and `close`. Use returned IDs.
Open confirms native identity, title and required tool readiness without a model
prompt. Resume uses native saved history; close and forget do not delete it.

Active sends require a matching native steer acknowledgment for `injected`.
If the native turn ended before that steer, its source-proven NotSubmitted
response causes the daemon to retain the original message and start the next
managed run. Idle messages start a managed run directly. A
`queued_for_next_turn` receipt is bounded daemon scheduling, not native
admission, durability, or model consumption. Other crossed-boundary or native
errors remain uncertain and are never replayed. Results are read without
consuming and acknowledged separately. Persistence and automatic close are
independent; see the installed generic skill for their exact fields and
collection rules.

Omitted or `default` `permission_mode` inherits Codex policy. Other native
approval-policy strings pass through without changing the inherited sandbox.
The explicit `bypassPermissions` value, lane `--yolo`, and lane
`--dangerously-bypass-approvals-and-sandbox` select App Server approval `never`
and sandbox `danger-full-access`; the TUI-only flags are consumed before the
App Server process is launched. Combining a raw bypass flag with a different
explicit policy is rejected as contradictory. A headless lane has no human
approval recipient; unsupported native
approval requests fail truthfully. Normal close ends stdin, drains native output
and waits for the direct native process. Hard failure uses forced cleanup; this
does not promise containment of arbitrary native tool descendants.

`never` prevents approval prompts; by itself it does not grant MCP tool access.
Every managed Codex peer and lane therefore projects the exact native policy
entry
`plugins.codex@sessionbus-peers.mcp_servers.sessionbus.tools.sessionbus.approval_mode="approve"`
alongside plugin activation. The entry grants the one Sessionbus tool and all
of its public actions; it does not change the approval policy, sandbox, or any
other tool. `--yolo` still maps to native
`--dangerously-bypass-approvals-and-sandbox`, and that global caller choice is
preserved alongside the fixed tool grant. Lanes project the equivalent policy
through App Server's thread and turn fields because its command does not accept
the TUI flag. Native `strict_auto_review` has a separate policy path.

Caller config remains byte-preserved. Assignments that disable plugin
activation, remove Sessionbus from `enabled_tools`, add it to `disabled_tools`,
replace an ancestor table, or change the exact tool policy away from `approve`
are rejected. Equivalent `true`/`approve` assignments and safe tool lists remain
valid. A server default approval setting also remains caller-owned because the
native field applies only when no exact tool override exists. `--disable
plugins` is rejected because it disables managed activation. Native CLI config
keys split literally on dots; quoted segments remain different keys. Other
plugins, servers, tool policies, feature flags, and values after native `--`
remain untouched.

## Interactive launch

```sh
codex-peer --group team -n review
codex-peer --resume "native session selector" -g team --group review
```

Repeated `-g`/`--group` values accumulate before native `--`, wherever supplied.
`--group=value` and comma-separated values are supported. `-n`/`--name` names
the initial native selection; native Codex owns resume/picker/fork selection.
`--resume` becomes native `resume`; `--yolo` becomes
`--dangerously-bypass-approvals-and-sandbox`. Other arguments keep their bytes
and order. After `--`, all arguments remain native operands.

The launcher starts one Go broker and execs the native TUI. That broker owns
one native App Server, two local endpoints and a Connection/Caller for each
loaded native thread. MCP helpers forward native thread metadata to those
owners. The launcher prefixes its native activation and `--remote` transport;
a caller's own `--remote` conflicts and is reported as an integration error.
The broker uses an OS parent-exit watch and closes native stdin on shutdown,
drains output and joins its owned child. It adds no shared server service,
polling loop, replay queue or native history lock.

Loaded native threads have separate peer rows. Rename updates the same row;
fork adds a new one. Clear can leave earlier threads loaded and addressable;
native thread closure or server loss withdraws their rows. Normal TUI quit and
abrupt TUI death removed the owned processes and endpoints in installed checks.
Killing the broker itself left stale private sockets and the TUI's native
reconnect screen; the bus rows and native server/MCP processes disappeared.
No individual child exit status or arbitrary-tool containment follows from
those absence checkpoints.

A message to an interactive thread is acknowledged with `written` as soon as the
broker holds it. That receipt does not mean native Codex has taken it. The broker
then hands messages to the native in arrival order: it steers the turn in
progress, or starts a turn when the thread is idle, including after a turn that
ended in an error. A steer that the native
refuses as an invalid request, for example because the turn had just ended or a
compaction was running, is kept and handed over again on the next message or
change of the thread's turn state. A steer or turn start that was sent to the
native and failed in any other way, or whose outcome is unknown, is not
repeated. The queue is in memory only: at most 64 messages or 1 MiB per thread
and 16 MiB per broker, beyond which a message is rejected with
`Sessionbus unsent input limit reached`. It ends with the thread's bus presence.

Native `-c`/`--config` occurrences are mirrored to App Server in order and kept
on the TUI. Other native options go to the TUI. In particular, profile options
remain on the TUI: native App Server rejects that CLI option. Server-level
profile settings and settings omitted by native remote projection do not gain
full standalone equivalence. The wrapper does not read or merge profiles.

The package includes `THIRD-PARTY-NOTICES.txt`, including the ISC notice for
the Go WebSocket implementation. No Node.js process or runtime is required.

## Remove

```sh
"$HOME/.local/libexec/sessionbus/codex/uninstall"
```

This runs native `plugin remove codex@sessionbus-peers` and removes only the
owned package, marketplace files and matching public symlink. The native
marketplace registration may remain; remove that registration with the native
marketplace command if desired. Other plugins and native history remain intact.

## Daemon outages

Interactive presence reconnects automatically after a daemon outage while the
native session remains alive. Calls made during the outage fail; interrupted
calls and deliveries are not replayed. Reconnection republishes the latest
native identity and title. Native session end, supersession and owner shutdown
remain terminal. Daemon-managed Worker lanes do not reconnect after losing
their launch connection.

When orchestrating another product, use the tool identifier and argument envelope
from the orchestrator's installed declaration. Use the selected lane product's
`describe` response and skill/README for its open fields, native permissions,
receipts and lifecycle. See the [shared delivery guidance](../README.md#delivery-and-presence)
for receipt uncertainty and the limits of presence flags.
