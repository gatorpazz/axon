# axon

A tool gateway and durable job runner for agent workloads, written in Rust.

Agents (or people) submit **jobs**: ordered plans of **tool** calls whose inputs
can reference earlier outputs. axon authorizes every call, runs the plan
durably, streams progress as it happens, and attaches **provenance** to every
result. The same tools are reachable over REST and over the
**Model Context Protocol**.

It is a compact, working slice of the kind of platform that sits behind AI
research tools: shared services, agent job execution, a tool gateway, an
event-driven data layer. The bundled tools query a **synthetic** cohort
(generated data, no real participants) and run real statistics on it.

```
            REST  /v1/jobs  /v1/tools          MCP  POST /mcp
                      │                              │
              ┌───────▼──────────────────────────────▼───────┐
              │  axum · bearer auth → PrincipalId            │  axon-server
              └───────┬──────────────────────────────┬───────┘
     submit / cancel  │                              │ invoke
              ┌───────▼────────┐  each step  ┌───────▼────────────────────┐
              │   JobRunner    ├────────────►│  Gateway                   │
              │ fold log → act │             │ authz · timeout · size cap │
              └───────┬────────┘             │ metrics · provenance       │  axon-core
        append (CAS)  │  ▲ notices           └───────┬────────────────────┘
              ┌───────▼──┴─────┐                     │ typed Tool impls
              │   EventStore   │              cohort.query · stats.describe
              │ memory │ Postgres (RLS, LISTEN/NOTIFY)   stats.welch_t_test
              └───────┬────────┘
                      └──► SSE  GET /v1/jobs/{id}/events  (replay, then live)
```

## Quick start

```sh
cargo run                      # in-memory store, dev credentials, 127.0.0.1:8080
open http://127.0.0.1:8080     # small console: submit a job, watch events stream in
```

With no access file the server enables two development principals and only
binds to loopback: `dev-alice-token` (every tool) and `dev-bob-token`
(`stats.describe` only).

```sh
H='Authorization: Bearer dev-alice-token'

# Submit a three-step pipeline: select two groups, t-test them, describe one.
ID=$(curl -s localhost:8080/v1/jobs -H "$H" -H 'content-type: application/json' \
       -d @examples/apoe4-ptau217.json | jq -r .id)

curl -N localhost:8080/v1/jobs/$ID/events -H "$H"    # SSE; ends when the job does
curl -s localhost:8080/v1/jobs/$ID -H "$H" | jq      # folded state + provenance

# The same tools over MCP
curl -s localhost:8080/mcp -H "$H" -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"tools/call",
  "params":{"name":"stats.describe","arguments":{"values":[1,2,3,4]}}}' | jq
```

For durable storage, point it at PostgreSQL (migrations run at startup):

```sh
AXON_DATABASE_URL=postgres://axon:secret@localhost/axon cargo run
```

Use a role that is neither superuser nor `BYPASSRLS`; PostgreSQL exempts those
from row-level security. Real credentials go in an access file
(`--access-file`, see [`access.example.toml`](access.example.toml)); `axon --help`
lists every option.

## API

| Method & path                     | What it does                                                    |
|-----------------------------------|-----------------------------------------------------------------|
| `POST /v1/jobs`                   | Validate and submit a job spec → `201` + `Location`             |
| `GET /v1/jobs`                    | The caller's jobs, newest first                                 |
| `GET /v1/jobs/{id}`               | Current state, folded from the event log                        |
| `GET /v1/jobs/{id}/events`        | SSE: replay after `Last-Event-ID` / `?after=`, then live tail   |
| `POST /v1/jobs/{id}/cancel`       | Cancel; `409` if already finished                               |
| `GET /v1/tools`                   | Tools the caller may invoke, with JSON Schemas                  |
| `POST /v1/tools/{name}/invoke`    | Call one tool directly → output + provenance                    |
| `POST /mcp`                       | MCP over stateless Streamable HTTP: `initialize`, `ping`, `tools/list`, `tools/call` |
| `GET /healthz`, `GET /metrics`    | Liveness; Prometheus counters                                   |

Errors are `{"error": {"code", "message"}}`. A job the caller does not own is a
`404`, never a `403`: the API does not confirm that other people's jobs exist.

## Design notes

**The log is the job.** A job is an append-only list of events
(`submitted`, `step_started`, `step_succeeded`, …). State is a pure fold over
them ([`job/state.rs`](crates/axon-core/src/job/state.rs)); the runner, the API
and the tests all derive it the same way. There is no in-memory job table, so
crash recovery is the ordinary code path: at startup every unfinished job is
re-driven from its log. Completed steps never re-run; a step interrupted
mid-flight is retried (at-least-once) up to three starts.

**One concurrency primitive.** Every append names the sequence number it
expects to follow. Cancellation is just an append that wins that race: the
runner's next write conflicts, it reloads, sees a terminal state and stops. A
store notice also aborts the tool call in flight, so a cancelled job does not
sit out a long step ([`runner.rs`](crates/axon-core/src/runner.rs)).

**No polling, no gaps.** Stores emit notices that only say "job J reached seq
N". A follower subscribes first, then reads from its last delivered `seq`, and
treats every notice (or a lagged receiver) as a reason to read again. The log
stays the single source of truth, access checks apply on every read, and the
replay→live handoff needs no lock
([`store/mod.rs`](crates/axon-core/src/store/mod.rs)). On PostgreSQL the notice
is a `NOTIFY` sent inside the appending transaction, so it also reaches other
replicas.

**Isolation in the database.** Each transaction declares its actor with a
transaction-local setting and row-level security policies hide everything else
([`0001_init.sql`](crates/axon-server/migrations/0001_init.sql)). The Rust
queries contain no `WHERE owner = …`. The tests go around the store with raw
SQL to prove the policies hold on their own.

**One gateway.** The runner, REST and MCP all call `Gateway::invoke`, which is
the only place a tool can be reached. It checks a Zanzibar-style relation
(`tool:stats.describe#invoker@group:researchers#member`), enforces a timeout
and an output cap, counts the outcome, and stamps provenance: tool, version,
caller, timing, and SHA-256 digests of input and output
([`gateway.rs`](crates/axon-core/src/gateway.rs),
[`authz.rs`](crates/axon-core/src/authz.rs)).

**Typed tools, erased at the edge.** A tool implements a trait with associated
`Input`/`Output` types; the JSON Schema is derived from `Input`, and
(de)serialization happens once in a generic adapter
([`tool.rs`](crates/axon-core/src/tool.rs)). Adding a tool is one `impl` and one
`.register(...)`.

## Testing

```sh
cargo test --workspace
AXON_TEST_DATABASE_URL=postgres://axon_test:axon@localhost/axon_test cargo test --workspace
```

- **Store contract**: one behavioural suite
  ([`store/contract.rs`](crates/axon-core/src/store/contract.rs)) runs against
  both the in-memory and PostgreSQL stores: ordering, compare-and-set under a
  real race, tenant isolation, notices, replay-then-tail.
- **Runner**: pipelines with references, step failure, authorization at
  execution time, cancellation pre-empting a 20 s step, resuming after the
  runner is killed mid-step, and the crash-loop guard.
- **HTTP**: the real router driven in-process: auth, SSE framing and resume,
  cross-tenant `404`s, validation errors, and an MCP session.
- **Numerics**: the t-distribution is checked against closed forms and SciPy
  reference values; property tests cover the statistics and the tuple parser.

PostgreSQL tests are skipped unless `AXON_TEST_DATABASE_URL` is set, and refuse
to run as a role that bypasses row-level security. CI
([`ci.yml`](.github/workflows/ci.yml)) runs fmt, clippy with warnings denied,
and the full suite against PostgreSQL.

## Layout

```
crates/axon-core     domain: authz, gateway, tools, job spec/events/state, store trait, runner
crates/axon-server   axum routes (REST, SSE, MCP), bearer auth, PostgreSQL store, web console
examples/            a sample job spec
```

## Known limits

- **One runner per database.** Racing runners cannot corrupt a log (appends are
  compare-and-set) but would duplicate work. Scaling out needs job leases.
- **Bearer tokens, not OIDC.** Authentication is a hashed-token lookup behind
  one function; JWT validation would replace it without touching anything else.
- **The `system` actor is a session setting.** A hardened deployment would give
  the runner its own database role instead.
- **Tools run in-process.** There is no sandbox for untrusted or generated
  code; the gateway's timeout and output cap are the only bounds.
- **Counters only.** Metrics are Prometheus counters and logs are structured;
  there are no histograms or distributed traces yet.
- Job listing is unpaginated, and `/metrics` is unauthenticated.

## License

[MIT](LICENSE)
