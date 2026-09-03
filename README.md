# termux-agent-dispatcher-go

Go rewrite of [termux-agent-dispatcher](https://github.com/DSamuelHodge/termux-agent-dispatcher)
(Python reference implementation — the behavioral spec).

A single static Go binary, cross-compiled for Termux/Android (aarch64, CGO off),
that turns typed verbs into Termux:API processes and speaks **MCP 2026-07-28
(stateless)** on `127.0.0.1:8477`. It is the phone's hands, not the planner:
wherever the LLM decision loop runs, it dispatches verbs without knowing
anything about tiers, risk, or subprocess mechanics.

## Guarantees carried over from the Python dispatcher (non-negotiables)

1. **Audit-before-execute.** Every attempt — `requested`, `approved`, `denied`,
   `executed`, `failed`, `timeout`, `stopped` — is written to `logs/audit.log`
   *before* the corresponding action, so a crash mid-execution still leaves a
   record of intent.
2. **Fail-closed confirmation.** For any verb in `confirmation_required_for`,
   a missing `termux-dialog` binary, a JSON decode failure, or a 120s timeout
   with no human response all count as **denial**, never approval.
3. **Validate before gating.** Malformed args fail before the risk gate — a
   human is never prompted to approve a call the machine can prove is broken.
   Ordering: unknown verb → route/tier contract → malformed payload → risk
   gate → execution.
4. **Circuit breaker keyed off durable storage** (`logs/agent.db`, WAL +
   `synchronous=FULL`) — trips on 5 timeout/failed rows inside 60s, cools
   down after 30s, survives process restarts, and stays live in *every*
   autonomy mode.
5. **Tier A vs Tier B.** Tier A: bounded, single request/response, timeout.
   Tier B: unbounded/streaming (`timeout: null`), start/poll/stop lifecycle
   with an opaque subscription handle; orphans from a prior crash are reaped
   on boot via `logs/subscriptions.pids`.
6. **`verbs.yaml` is the single source of truth.** Add a verb to the YAML and
   it is live everywhere — tool registration, risk gating, execution — with
   no new code. Both implementations load the same file.

## High-risk confirmation: on-device only

`termux-dialog` on the physical device is the **sole approval surface**. The
MCP client is never the approver — that would collapse requester and approver
onto the same channel and remove the physical-presence property the gate
exists for.

Flow for a high-risk call:

1. `tools/call` returns immediately with a pending handle:
   `{"confirm_id": "...", "status": "pending", "code": "CONFIRM_PENDING", ...}`
2. A goroutine shows the confirm dialog on the phone ("Allow: Send an SMS?")
   — the human taps Yes/No.
3. The decision is audited, the job resolves, and the client polls the
   `confirm.poll` tool with the `confirm_id`.
4. Pending rows left by a previous process are failed on boot (a dialog
   cannot resume across a restart).

## Autonomy modes

| Mode | Confirms on |
|---|---|
| `manual` | risk `medium` and `high` |
| `gated` | the catalog's `confirmation_required_for` (default: `high`) |
| `full` | nothing — but audit logging and the circuit breaker stay unconditional |

Switching modes is itself always risk `high`, confirmed on-device via the
same dialog, keyed off the mode being switched *from*. The choice persists
in `.autonomy-mode` across reboots.

## Layout

```
cmd/agentd/          entrypoint: wake-lock, orphan recovery, serve
internal/catalog/    verbs.yaml loader + verb spec (mirrors dispatch/catalog.py)
internal/riskgate/   audit log, autonomy mode, on-device confirm, check()
internal/confirm/    async confirm jobs, orphans, webhooks (mirrors confirm.py)
internal/store/      pure-Go sqlite (modernc.org/sqlite): events, breaker,
                     confirm jobs, idempotency
internal/tiera/      bounded exec + timeout
internal/tierb/      subscription manager, orphan pid tracking
internal/errs/       stable machine-readable error codes
internal/mcpserver/  MCP 2026-07-28 server, tool registration, dispatch
boot/01-start-agent  Termux:Boot unit
verbs.yaml           shared source of truth with the Python implementation
```

## Build

```
CGO_ENABLED=0 GOOS=android GOARCH=arm64 \
  go build -trimpath -ldflags="-s -w" -o agentd-android-arm64 ./cmd/agentd
```

No cgo, no NDK toolchain, no runtime interpreter — `modernc.org/sqlite` is
pure Go. Host build: `go build ./cmd/agentd`.

## Deploy (on-device Termux)

1. Push `agentd-android-arm64` and `verbs.yaml` to `~/agent/`, make the
   binary executable (`chmod 755`).
2. Install `boot/01-start-agent` at `~/.termux/boot/01-start-agent`
   (`chmod +x`). It exports the Termux `PREFIX`/`PATH` (Termux:Boot does not
   inherit a full environment) and starts `~/agent/agentd` under `nohup`.
3. Auth: token from `AGENT_TOKEN` env, else generated once into
   `~/agent/.agent-token` (`chmod 600`). Loopback-only is **not** private on
   Android — any app can dial 127.0.0.1 — so this token is the actual access
   control. Hand it to the brain with
   `cat ~/agent/.agent-token`; rotate by deleting the file and restarting.

## Smoke test

```
TOKEN=$(cat ~/agent/.agent-token)
curl -s -H "X-Agent-Token: $TOKEN" http://127.0.0.1:8477/health
# MCP: list tools
curl -s -X POST -H "X-Agent-Token: $TOKEN" -H "Content-Type: application/json" \
  http://127.0.0.1:8477/mcp \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
# Tier A perceive
curl -s -X POST -H "X-Agent-Token: $TOKEN" -H "Content-Type: application/json" \
  http://127.0.0.1:8477/mcp \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"battery.status","arguments":{}}}'
# High-risk act — returns a pending confirm_id; the dialog is on-device;
# poll with confirm.poll
curl -s -X POST -H "X-Agent-Token: $TOKEN" -H "Content-Type: application/json" \
  http://127.0.0.1:8477/mcp \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"sms.send","arguments":{"number":"+15551234567","text":"hi","idempotency_key":"demo-1"}}}'
```

## Tests

```
go test ./...
```

The test catalog uses only POSIX commands (`echo`, `false`, `sh`), so the
suite runs on macOS/Linux CI as well as in Termux. On-device dialog behavior
is covered by `riskgate.ConfirmDeviceFn` stubs; live Termux:API behavior is
verified by hand against a real device.

## Deviations from the Python reference (explicit, not silent)

- `tierb.isOurOrphan` scans **all** argv elements for a `termux-*` basename.
  Python checks `argv[0]` only — on Termux every termux-* tool is a bash
  script, so `argv[0]` is the interpreter and Python's orphan guard never
  matches its own wrapped children. The match set is unchanged (still only
  `termux-*` tools), so the pid-reuse guard is not weakened.
- `autonomy.set_mode` persists its choice to `.autonomy-mode` so a reboot
  keeps it (the Python reference predates autonomy modes entirely).
- MCP framing is provided by the official Go SDK (`modelcontextprotocol/go-sdk`)
  in stateless mode rather than a hand-rolled JSON-RPC layer; the Python
  implementation's strict header checks are the reference for its own
  transport, not a behavioral guarantee the SDK cannot express.
