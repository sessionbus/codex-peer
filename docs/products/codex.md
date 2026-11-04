# Codex product facts

> Historical source note: citations to pre-split Sessionbus paths resolve in
> the Forgejo `ai/sessionbus` repository through its `legacy-*` branches.
> Citations to product source resolve in the external repository and full
> commit recorded by the split archive manifest. Host evidence paths are
> immutable external artifacts, not repository paths.

## File transfer binding (2026-10-28)

The core SDK owns the transfer engine and its direct filesystem executor;
`go.mod` pins the exact core revision. codex-peer only binds it:

- **Lane.** `NewWorker` enables transfers by default and `worker.Caller()`
  routes `transfer.*` locally. The lane keeps the thread's effective cwd that
  `thread/start`/`thread/resume` proved at open.
- **Interactive broker.** Each native thread's resident owns one
  `NewConnectionTransfers(nil)`. Its hello goes through `HelloObserved`, the
  SDK's own requests through `HandleConnectionRequest` on the reader, its
  caller is `NewCallerWithTransfers`, and `CloseConnectionGeneration` runs
  when a connection ends and when the resident retires.
- **Paths.** The SDK opens a path as given, so a relative path would resolve
  against the broker or worker process's directory. The tool owner joins a
  relative `source`/`destination` to the thread's cwd (without cleaning, so
  the filesystem resolves links and `..`) and refuses it when the cwd is
  unknown.
- **Final notices.** A transfer's final notice for a file this thread
  received gains its local destination through `ProjectDelivery`. That
  projection can wait for the publication in flight and for the transfer's
  resources to be released, which needs the connection's reader, so the
  broker runs it off the reader before queueing the notice. A message arriving during that short wait can be queued before
  the notice; every other delivery keeps its arrival order.

## Current Go candidate, 2026-09-09

Mandatory-wake source update (2026-09-21): an idle lane delivery returns
NotRunning before native submission so daemon v0.5.7 seeds a managed Run. An
active lane still uses native `turn/steer` with the exact expected turn ID;
only the two source-proven `SteerSubmission::NotSubmitted` responses map to
NotRunning, while every other failure remains uncertain. Interactive idle
`turn/start` and active `turn/steer` are unchanged. See
[the all-product boundary](../designs/mandatory-message-wake-20260921/NATIVE-BOUNDARIES.md).

Current installation and policies are described in `codex/README.md` and
`docs/designs/codex-0.5.0/{LANE-SELECTION,INTERACTIVE-SELECTION}.md`. The older
facts below retain their original version and topology. In particular, the old
shared-daemon restriction on per-launch groups and the old archive-on-close
behavior are historical integration choices, not current Codex limitations.

- Native CLI override keys split literally on dots; embedded quotes around the
  plugin ID prevent activation. The working per-launch key is
  `plugins.codex@sessionbus-peers.enabled=true`. Native `config/value/write`
  has a different quote-aware key parser. The original failed no-input Open and
  corrected successful Open are preserved in `dev1-lane-open-{ebd5c0a,aaea430}`
  under `/home/antst/codex-architecture-20260909/`.
- Retained native 0.153.4 source commit
  `3d2ee51ca2d5db578f328aa75e20aa22c0197c9a` defines repeatable
  `--disable FEATURE` as equivalent to `-c features.<name>=false`, validates the
  feature name, and folds the generated override into the same root config
  overrides (`sources/native-01534/codex-rs/cli/src/main.rs:959-1006,1052-1059`
  under `/home/antst/codex-architecture-20260909/`; commit and file hash are in
  that directory's `SOURCE.json`). The managed installed rows cited here use
  the accepted `plugins` feature through `features.plugins=true`; therefore
  `--disable plugins` is the native spelling of `features.plugins=false`.
- At that exact native commit, `codex-rs/config/src/mcp_types.rs:68-78`
  defines `McpServerToolConfig` with only `approval_mode` and
  `output_token_limit`. It has no `enabled` field. The official source is
  `https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/config/src/mcp_types.rs`.
  The managed argument guard therefore reserves the real approval control but
  does not invent an `enabled` control for a per-tool entry.
- Managed value checks mirror native `CliConfigOverrides`: decode the wrapped
  `_x_ = <raw>` TOML value, then fall back to a whitespace-trimmed raw string
  with leading/trailing quote characters removed when decoding fails
  (`codex-rs/utils/cli/src/config_override.rs:47-102` at the same commit). The
  Go implementation uses `github.com/BurntSushi/toml` v1.6.0, a pure-Go MIT
  library (`COPYING` SHA-256
  `d21cb1c60785d6d3a84a7059323ccafc45c645b1bbda281c76a62d66ad2d7dc3`).
  It adds no transitive module, executable, service, configuration, or runtime
  installation dependency.
- Native 0.153.4 `AppServerCommand` does not define the TUI
  `--dangerously-bypass-approvals-and-sandbox` flag
  (`codex-rs/cli/src/main.rs:546-595`). The TUI maps that flag to approval
  `Never` and sandbox `DangerFullAccess` separately (`main.rs:2212-2225`). App
  Server accepts those settings on `thread/start` and converts them to native
  config overrides
  (`codex-rs/app-server/src/request_processors/thread_processor.rs:1116-1192,1604-1631`).
  The 0.4.0 lane likewise canonicalized the raw flag to `bypassPermissions`
  before dispatch and mapped bypass to `never` plus `danger-full-access`
  (`ff81565:cmd/agent-sessions/lane.go:280` and
  `ff81565:internal/products/codex/lane.go:156`). Current lanes retain that
  policy projection and never pass the TUI flag to App Server.
- On installed aaea430/native0.153.4, idle `thread/inject_items` staging received
  `queued_for_next_turn`; the marker appeared in native user history before any
  assistant response and was consumed in the following explicit run. The
  receipt is admission, not consumption. Evidence: `dev1-lane-stage-aaea430`.
- An omitted approval policy yielded native MCP rejection. Explicit `never`
  still rejected a tool requiring approval. Caller-selected per-plugin per-tool
  `approval_mode="approve"` allowed the public Sessionbus call; it grants that
  tool's actions, not only list. No default policy or read-only annotation was
  substituted. Evidence: `dev1-lane-{wake,grant}-aaea430`.
- A message with `idle_message:"run"` started a shared native run, whose
  retained result was collected, acknowledged and then closed. Successful
  Open itself submitted no model input. Normal close preserved saved native
  history. Four identified worker/native/configured-helper processes were
  absent after close in the grant row; no direct exit-status or arbitrary
  descendant-containment claim is made.

Interactive installed9b rows establish actual parent-to-lane lifecycle and
completion-pointer collection, idle/active admission, mixed groups, native
rename/fork/clear/resume and ordinary isolation. Normal quit and abrupt TUI death
removed the identified owned processes/endpoints. Broker SIGKILL instead left
stale socket files and the TUI's native reconnect UI; the bus row and native
server/helpers disappeared. Operator removal of those exact stale paths is
separate from product cleanup. Shared Claude smoke passed on the rebuilt real
installation. See `docs/designs/codex-0.5.0/ACCEPTANCE.md` and its manifest index
for exact limits, including controlled-only cancellation/lane interrupt/settings
coverage and inherited transcript attribution. Whole combined
9b833c3 archive size is 2,978,023 bytes, with one 7,135,394-byte linked executable
and 7,157,883 bytes across regular payload files. The increase relative to the
lane checkpoint includes the entire broker integration, not just the WebSocket
dependency. Build/member hashes are in `dev1-combined-entry/BUILD.json`.

## Retained earlier native and integration evidence

- `codex app-server --stdio` accepts newline-delimited JSON-RPC requests and emits response objects without a `jsonrpc` member. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- An App Server client initializes with `initialize`, then sends the `initialized` notification. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- `thread/start` with `ephemeral:false`, `serviceName:"codex-peer"`, and `historyMode:"legacy"` returns the product-owned thread UUID. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- `thread/start` reports the effective model, cwd, approval policy, sandbox, reasoning effort, and thread record at the top level. — verified: 0.153.2 — source: `/home/antst/agentbus-evidence/codex-20260906T071535Z/P0-appserver.jsonl`
- A newly created and named zero-turn legacy thread is materialized by `thread/resume` with `excludeTurns:true`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- `thread/resume` reports the effective model, cwd, approval policy, sandbox, reasoning effort, and thread record at the top level. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- `turn/start` returns a turn ID before the matching `turn/started` notification. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- `turn/started` carries `threadId` and a turn object whose running status is `inProgress`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- `turn/steer` accepts `threadId`, `expectedTurnId`, and input, then returns the same `turnId`; the steered text affects the final answer. — verified: 0.153.2 — source: `/home/antst/agentbus-evidence/codex-p1-20260906T071721Z/P1-appserver.jsonl`
- `turn/interrupt` before `turn/started` fails with code `-32600` and message `no active turn to interrupt`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074006Z/P3-appserver.jsonl`
- `turn/interrupt` after `turn/started` returns `{}`, then `turn/completed` reports status `interrupted`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`
- A completed `turn/completed` notification carries summary items, timestamps, nullable error, and an `agentMessage` item with phase `final_answer`. — verified: 0.153.2 — source: `/home/antst/agentbus-evidence/codex-20260906T071535Z/P0-appserver.jsonl`
- Per-thread `config.mcp_servers.<name>.url` starts a streamable HTTP MCP client that sends initialize, initialized, tools/list, and tools/call without a session header. — verified: 0.153.2 — source: `/home/antst/agentbus-evidence/codex-20260906T071535Z/P0-http.jsonl`
- MCP `tools/call` metadata includes `_meta.threadId` and `x-codex-turn-metadata.thread_id`, both equal to the native thread UUID. — verified: 0.153.2 — source: `/home/antst/agentbus-evidence/codex-20260906T071535Z/P0-http.jsonl`
- The 0.4.0 connector takes Codex identity from MCP `_meta.threadId`, never from tool arguments. — verified: 0.4.0 — source: `ff81565:cmd/agent-sessions/connector.go:247`
- The 0.4.0 connector delivers to an active interactive thread with `turn/steer` and wakes an idle thread with `turn/start`. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:479`
- The 0.4.0 launcher starts `codex app-server daemon` and runs the TUI through `--remote unix://<control-socket>`. — verified: 0.4.0 — source: `internal/launcher/codex_peer.go:129`
- Informational and non-TUI subcommand invocations pass through with their original argv and environment; `resume` and `fork` are coordinated TUI selectors and receive the wrapper-owned `--remote` address. — verified: 0.4.0 — source: `internal/launcher/codex_peer.go:122`
- The Codex interactive option table records value-taking `--remote`, `--remote-auth-token-env`, `-i`/`--image`, `--local-provider`, and `--add-dir`; a wrapper must preserve their value arity when projecting its own flags. — verified: 0.4.0 — source: `internal/launcher/options.go:18`
- The 0.4.0 lane validates effective approval after `thread/start`, validates effective cwd and approval after `thread/resume`, and applies approval and sandbox through `thread/settings/update`. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:224`
- The 0.4.0 lane disables `features.code_mode_host` so headless tool calls stay in the App Server. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:713`
- The 0.4.0 App Server bridge handles `mcpServer/elicitation/request` and `item/tool/call`; it rejects other headless server requests with `-32601`. — verified: 0.4.0 — source: `internal/bridge/dynamic_tools.go:26`
- The 0.4.0 observer recognizes `thread/started`, `turn/started`, `turn/completed`, `thread/status/changed`, and `thread/name/updated`. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:904`
- The 0.4.0 resume path avoids a second `thread/resume` while its exact App Server client already owns the loaded thread. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:355`
- The 0.4.0 recovery path can unarchive a thread, restore loaded subscriptions after reconnect, and resolve a durable active turn with `thread/turns/list`. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:409`
- The 0.4.0 close path archives the exact thread and unsubscribes so its stdio MCP child is released. — verified: 0.4.0 — source: `internal/bridge/codex_native.go:759`
- A turn started with the pre-rename unsupported-model probe name preserved in the captured frame below closes with status `failed`; its error object carries `message`, `codexErrorInfo`, `additionalDetails`, and `misalignment`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-failed-turn-20260906T085803Z/appserver.jsonl`
- For an interactive TUI connected through `--remote`, command-line `mcp_servers.sessionbus.command` and `.args` do not override an installed same-name plugin MCP entry: Codex spawned the installed legacy connector and did not spawn the configured `codex-peer mcp`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer/C1-failure-processes.txt`
- In remote mode the App Server daemon spawns MCP servers from the configuration it loaded; changing the installed `mcp_servers.sessionbus` entry requires restarting that daemon before an interactive TUI can use the replacement. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer/C1-installed-mcp-configs.txt`
- With the global entry set to `command = "codex-peer"`, `args = ["mcp"]`, and the App Server daemon restarted, Codex started `sessionbus` but its MCP client timed out after 30 seconds and published no peer presence. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer-rerun/C1-pane-final.txt`
- Before lazy peer initialization, `codex-peer mcp` with no `SESSIONBUS_*` environment consumed an MCP initialize request but wrote zero stdout bytes and ended with `context canceled` at the probe's five-second bound; it tried to connect to the native App Server before serving MCP. — verified: codex-peer 8323584 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer-rerun/manual-no-env`
- The App Server daemon resolves stdio MCP commands using the environment that started the daemon, which may omit the user's login binary directory; a bare `codex-peer` command then fails with `No such file or directory`, so the installed MCP entry uses the absolute installed executable path. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer-fixed/C1-blocking-pane.txt`
- The App Server control Unix socket accepts `GET /rpc` with a WebSocket upgrade, returns `101 Switching Protocols`, and exchanges masked client text frames with unmasked server text frames; a direct initialize reports Codex 0.153.4 without spawning a proxy subprocess. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer-absolute/ws-exchange.jsonl`
- Codex can assign a natural-language thread title containing spaces; `thread/read` returned `List agent sessions` after the matching tool prompt. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer-direct/manual-mcp-strace/hello-invalid.jsonl`
- With the expanded bus name grammar, a daemon-spawned MCP helper hellos under the verbatim sentence title `List agent sessions` and its `sessionbus` tool completes successfully. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C1-name-grammar/C1-pane.txt`
- A remote TUI's MCP child inherits the App Server daemon's installed MCP environment, not the launcher's `SESSIONBUS_GROUPS`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C1-name-grammar/mcp-agentbus-env-redacted.txt`
- An observer in the launcher's requested `codex-cells` group could not see that child-backed peer when the installed MCP entry supplied no `SESSIONBUS_GROUPS`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C1-name-grammar/C1-observer-list.json`
- A TUI `-c sessionbus_groups=["codex-cells"]` override is absent from both `thread/read` and `config/read` over the control socket; `thread/read` exposes `extra:null` but no settable per-thread metadata carrier. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C1-groups-probe/ws-frames-after-quit.jsonl`
- Interactive Codex peers share the group array stored in the installed `sessionbus` MCP entry; the default is `[]`, and per-launch `-g/--group` is unavailable because several TUI sessions share one daemon and one `CODEX_HOME`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C1-groups-probe/ws-summary-after-quit.json`
- `/clear` starts a second daemon-owned MCP helper for the new thread; five seconds after the new helper connected, the old helper and old thread identity remained connected alongside it rather than the original helper receiving the new `_meta.threadId`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C8-peer-identity/list-after-replace-plus-5s.json`
- A delivery addressed to the old pre-`/clear` title returned `injected` for the old thread ID, entered that thread's rollout with the exact Sessionbus carrier, and produced `Received C8_OLD_DELIVERY.` there. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C8-peer-identity/old-thread-rollout-match.txt`
- An archived thread is reported by `thread/read` with its rollout path directly under `archived_sessions`; its status alone remains `notLoaded`. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-failed-turn-20260906T085803Z/read-frames.jsonl`
- After both test threads were archived and their two MCP helpers exited, the interactive product's reusable `codex-code-mode-host` remained under its host-wide App Server daemon; it is product-owned and carries no Sessionbus identity. — verified: 0.153.4 — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C9-peer-resume-errors/residual-code-mode-host.txt`
- `UNVERIFIED:` Codex could provide per-thread peer groups if a future TUI makes the existing `thread.extra` field user-settable and readable by the daemon-spawned MCP helper.
- `UNVERIFIED:` An active `thread/read` response and the one-row `thread/turns/list` response used by interactive peer delivery have not yet been captured on 0.153.x.

## Exact captured frames typed by the wrapper

Each line below is copied byte-for-byte from the `raw` field in the named evidence file.

- Initialize response — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"id":1,"result":{"userAgent":"agentbus-probe/0.153.4 (Ubuntu 24.4.0; x86_64) unknown (agentbus-probe; 1)","codexHome":"/home/antst/.codex","platformFamily":"unix","platformOs":"linux"}}
```

- Native error response — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074006Z/P3-appserver.jsonl`

```json
{"error":{"code":-32600,"message":"no active turn to interrupt"},"id":6}
```

- `thread/start` result, including the effective cwd, approval, and sandbox — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"id":2,"result":{"thread":{"id":"01a075a9-5eb8-7540-95ef-c8d3ffb8a752","extra":null,"sessionId":"01a075a9-5eb8-7540-95ef-c8d3ffb8a752","forkedFromId":null,"parentThreadId":null,"preview":"","ephemeral":false,"section":null,"sectionEnteredAt":null,"projectId":null,"historyMode":"legacy","modelProvider":"openai","model":"gpt-6-astra","reasoningEffort":null,"createdAt":1788680429,"updatedAt":1788680429,"recencyAt":1788680429,"status":{"type":"idle"},"path":"/home/antst/.codex/sessions/2026/09/06/rollout-2026-09-06T07-40-29-01a075a9-5eb8-7540-95ef-c8d3ffb8a752.jsonl","cwd":"/home/antst/agentbus-evidence/codex-p3-20260906T074029Z","cliVersion":"0.153.4","source":"vscode","canAcceptDirectInput":true,"threadSource":null,"agentNickname":null,"agentRole":null,"gitInfo":null,"name":null,"turns":[]},"model":"gpt-6-astra","modelProvider":"openai","serviceTier":null,"cwd":"/home/antst/agentbus-evidence/codex-p3-20260906T074029Z","runtimeWorkspaceRoots":["/home/antst/agentbus-evidence/codex-p3-20260906T074029Z"],"instructionSources":[],"approvalPolicy":"never","approvalsReviewer":"user","sandbox":{"type":"readOnly","networkAccess":false},"activePermissionProfile":{"id":":read-only","extends":null},"reasoningEffort":null,"multiAgentMode":"explicitRequestOnly"}}
```

- `thread/resume` result — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"id":4,"result":{"thread":{"id":"01a075a9-5eb8-7540-95ef-c8d3ffb8a752","extra":null,"sessionId":"01a075a9-5eb8-7540-95ef-c8d3ffb8a752","forkedFromId":null,"parentThreadId":null,"preview":"","ephemeral":false,"section":null,"sectionEnteredAt":null,"projectId":null,"historyMode":"legacy","modelProvider":"openai","model":"gpt-6-astra","reasoningEffort":null,"createdAt":1788680429,"updatedAt":1788680430,"recencyAt":1788680429,"status":{"type":"idle"},"path":"/home/antst/.codex/sessions/2026/09/06/rollout-2026-09-06T07-40-29-01a075a9-5eb8-7540-95ef-c8d3ffb8a752.jsonl","cwd":"/home/antst/agentbus-evidence/codex-p3-20260906T074029Z","cliVersion":"0.153.4","source":"vscode","canAcceptDirectInput":true,"threadSource":null,"agentNickname":null,"agentRole":null,"gitInfo":null,"name":"codex-p3","turns":[]},"model":"gpt-6-astra","modelProvider":"openai","serviceTier":null,"cwd":"/home/antst/agentbus-evidence/codex-p3-20260906T074029Z","runtimeWorkspaceRoots":["/home/antst/agentbus-evidence/codex-p3-20260906T074029Z"],"instructionSources":[],"approvalPolicy":"never","approvalsReviewer":"user","sandbox":{"type":"readOnly","networkAccess":false},"activePermissionProfile":{"id":":read-only","extends":null},"reasoningEffort":null,"multiAgentMode":"explicitRequestOnly","initialTurnsPage":null,"turnsBackwardsCursor":null,"itemsBackwardsCursor":null}}
```

- `turn/start` result — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"id":5,"result":{"turn":{"id":"01a075a9-638a-77f2-9722-3bc09dce6494","items":[],"itemsView":"notLoaded","status":"inProgress","error":null,"startedAt":null,"completedAt":null,"durationMs":null}}}
```

- `turn/started` notification — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"method":"turn/started","params":{"threadId":"01a075a9-5eb8-7540-95ef-c8d3ffb8a752","turn":{"id":"01a075a9-638a-77f2-9722-3bc09dce6494","items":[],"itemsView":"notLoaded","status":"inProgress","error":null,"startedAt":1788680430,"completedAt":null,"durationMs":null}},"emittedAtMs":1788680430493}
```

- `turn/steer` result — source: `/home/antst/agentbus-evidence/codex-p1-20260906T071721Z/P1-appserver.jsonl`

```json
{"id":4,"result":{"turnId":"01a07594-3310-78a0-a9a5-8afe4689eea5"}}
```

- `turn/interrupt` result — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"id":6,"result":{}}
```

- Completed `turn/completed` notification with a final-answer item — source: `/home/antst/agentbus-evidence/codex-p1-20260906T071721Z/P1-appserver.jsonl`

```json
{"method":"turn/completed","params":{"threadId":"01a07594-327d-7be1-ba09-0047b18e8770","turn":{"id":"01a07594-3310-78a0-a9a5-8afe4689eea5","items":[{"type":"agentMessage","id":"msg_09664ff8842ada9c016a9d13a9630c87d281743b358bd23e90","text":"STEER_PROBE_OK","phase":"final_answer","memoryCitation":null,"delivery":null,"questions":null}],"itemsView":"summary","status":"completed","error":null,"startedAt":1788679041,"completedAt":1788679081,"durationMs":39942}},"emittedAtMs":1788679081756}
```

- Interrupted `turn/completed` notification — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"method":"turn/completed","params":{"threadId":"01a075a9-5eb8-7540-95ef-c8d3ffb8a752","turn":{"id":"01a075a9-638a-77f2-9722-3bc09dce6494","items":[],"itemsView":"notLoaded","status":"interrupted","error":null,"startedAt":1788680430,"completedAt":1788680430,"durationMs":22}},"emittedAtMs":1788680430509}
```

- Failed `turn/completed` notification — source: `/home/antst/agentbus-evidence/codex-failed-turn-20260906T085803Z/appserver.jsonl`

```json
{"method":"turn/completed","params":{"threadId":"01a075f0-66ba-7b52-87a7-5dca3de53066","turn":{"id":"01a075f0-6737-7b02-ae86-c9be43e17c19","items":[],"itemsView":"notLoaded","status":"failed","error":{"message":"{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\"message\":\"The 'agentbus-invalid-model' model is not supported when using Codex with a ChatGPT account.\"}}","codexErrorInfo":"other","additionalDetails":null,"misalignment":null},"startedAt":1788685084,"completedAt":1788685085,"durationMs":1261}},"emittedAtMs":1788685085742}
```

- `thread/read` result with the captured `notLoaded` status — source: `/home/antst/agentbus-evidence/codex-failed-turn-20260906T085803Z/read-frames.jsonl`

```json
{"id":2,"result":{"thread":{"id":"01a075f0-66ba-7b52-87a7-5dca3de53066","extra":null,"sessionId":"01a075f0-66ba-7b52-87a7-5dca3de53066","forkedFromId":null,"parentThreadId":null,"preview":"Reply exactly NEVER.","ephemeral":false,"section":null,"sectionEnteredAt":null,"projectId":null,"historyMode":"legacy","modelProvider":"openai","model":"agentbus-invalid-model","reasoningEffort":null,"createdAt":1788685084,"updatedAt":1788685084,"recencyAt":1788685084,"status":{"type":"notLoaded"},"path":"/home/antst/.codex/archived_sessions/rollout-2026-09-06T08-58-04-01a075f0-66ba-7b52-87a7-5dca3de53066.jsonl","cwd":"/home/antst/agentbus-evidence/codex-failed-turn-20260906T085803Z","cliVersion":"0.153.4","source":"vscode","canAcceptDirectInput":null,"threadSource":null,"agentNickname":null,"agentRole":null,"gitInfo":null,"name":null,"turns":[]}}}
```

- Authoritative full `thread/turns/list` result — source: `/home/antst/agentbus-evidence/codex-failed-turn-20260906T085803Z/read-frames.jsonl`

```json
{"id":3,"result":{"data":[{"id":"01a075f0-6737-7b02-ae86-c9be43e17c19","items":[{"type":"userMessage","id":"item-1","clientId":null,"content":[{"type":"text","text":"Reply exactly NEVER.","text_elements":[]}]}],"itemsView":"full","status":"failed","error":{"message":"{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\"message\":\"The 'agentbus-invalid-model' model is not supported when using Codex with a ChatGPT account.\"}}","codexErrorInfo":"other","additionalDetails":null,"misalignment":null},"startedAt":1788685084,"completedAt":1788685085,"durationMs":1261}],"nextCursor":null,"backwardsCursor":"{\"turnId\":\"01a075f0-6737-7b02-ae86-c9be43e17c19\",\"includeAnchor\":true}"}}
```

- `thread/unsubscribe` result; empty mutation results use the captured `turn/interrupt` shape above — source: `/home/antst/agentbus-evidence/codex-p3-20260906T074029Z/P3-appserver.jsonl`

```json
{"id":8,"result":{"status":"notLoaded"}}
```

## Wrapper differences from the 0.4.0 source

- `CONTRADICTION:` The failed peer probe assumed TUI `-c` values would override the remote daemon's MCP configuration. Codex 0.153.4 instead used the entry already loaded by that daemon; installation now removes any same-name entry, adds the exact global `sessionbus` entry through `codex mcp`, and restarts the daemon unconditionally. — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/peer/C1-installed-mcp-configs.txt`
- `CONTRADICTION:` The current peer launcher starts the App Server daemon and injects `--remote` only for interactive TUI launches; informational and native subcommand invocations pass through unchanged. The 0.4.0 launcher used the same branch split but also carried pending-launch coordination that the universal peer removes. — source: `internal/launcher/codex_peer.go:122`
- `CONTRADICTION:` The current lane passes its approval, sandbox, model, and private MCP config through thread requests; the 0.4.0 bridge also issued `thread/settings/update` after resume. — source: `internal/bridge/native_support.go:84`
- `CONTRADICTION:` The current App Server client rejects every server request with `-32601`; the 0.4.0 bridge handled Sessionbus dynamic tool calls and elicitation inside the App Server connection. The universal wrapper instead supplies tools through the private stdio MCP child. — source: `internal/bridge/dynamic_tools.go:26`
- `CONTRADICTION:` The current notification reader consumes only exact `turn/started` and `turn/completed` frames. The 0.4.0 observer also projected thread start, status, and name events for host-global coordination. — source: `internal/bridge/codex_native.go:904`
- `CONTRADICTION:` The current wrapper owns one App Server child and exits on its death. The 0.4.0 host-global bridge reconnected, restored loaded subscriptions, and could unarchive recovery targets. — source: `internal/bridge/codex_native.go:409`
- `CONTRADICTION:` The earlier wrapper picture assigned `/clear` to `Peer.Replace`. Codex 0.153.4 instead starts a separate helper for the new thread and retains the old thread's helper and presence; the Codex peer backend therefore never replaces identity. — source: `/home/antst/agentbus-evidence/codex-cells-20260906T094434Z/C8-peer-identity/mcp-process-details.txt`
