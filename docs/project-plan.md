# Wici project plan

Status: early development. The workspace, the code checks, and the command
lifecycle in `wici-protocol` exist. All other parts of this plan are planned
work and are not implemented.

## 1. Purpose

Wici is an open-source Rust system that connects applications to AI agents on
other machines. An application sends a command. An agent on a laptop, a server,
or a cloud machine executes it. Events, approval requests, and results come back
to the application through one protocol.

Wici solves these problems:

- A command must not get lost if the network, the server, or a machine stops.
- A retry must not execute a command twice.
- A client that reconnects must get all durable events that it missed.
- An agent must not do a risky action without an approval.
- A user must always see the true state of a command, also when it is unknown.

Example: an application on a laptop sends "refactor module X" to an agent on a
home server. The laptop goes to sleep. The agent asks for approval to run a
migration. The user approves on a phone. The laptop wakes and shows the result.

Wici is not a database sync engine, a model framework, or a workflow platform.
Wici does not depend on one model vendor, one agent, or one cloud provider.

## 2. Concepts

| Term | Meaning |
| --- | --- |
| Tenant | An isolated owner of clients, runners, and data |
| Client | An application that sends commands and reads events |
| Runner | A process that executes commands with an agent |
| Session | An ordered stream of commands and events for one task |
| Command | A request from a client, with a unique ID |
| Event | A record in a session stream, with an ordered position |
| Approval | A decision of a client that lets an agent continue |
| Artifact | A file that a runner produces, stored with a checksum |

## 3. Components

One repository and one Cargo workspace. Add a crate only when code needs it.

| Crate | Responsibility | Status |
| --- | --- | --- |
| `wici-protocol` | Wire types, versions, validation, command lifecycle | Started |
| `wici-core` | Delivery and recovery state machines, storage traits | Planned |
| `wici-store-sqlite` | Local outbox, inbox, cursors, execution records | Planned |
| `wici-store-postgres` | Server storage, tenancy, delivery records | Planned |
| `wici-client` | Connection, subscriptions, reconnect, replay | Planned |
| `wici-server` | Authentication, authorization, routing, acceptance | Planned |
| `wici-runner` | Agent adapters, process lifecycle, execution records | Planned |
| `wici-ffi` | C ABI for native applications | Planned |

Dependencies point inward. Adapters depend on core contracts. Protocol types do
not import database, network, agent, or UI types.

An application can embed Wici as a library or use the standalone binaries. An
agent never runs inside the UI process of the host application.

## 4. Command lifecycle

These states are visible to clients. `wici-protocol` implements them as
`CommandState`.

| State | Meaning |
| --- | --- |
| `queued_local` | Saved on the client, not yet accepted by the server |
| `accepted_durable` | Committed by the server |
| `received_by_runner` | A runner has the command, execution has not started |
| `running` | The runner recorded the execution intent and started |
| `awaiting_approval` | The agent waits for an approval decision |
| `completed` | Finished, the result is saved |
| `failed` | Rejected, expired, or finished with an error |
| `cancelled` | Stopped by a cancel request before completion |
| `outcome_unknown` | The runner stopped during execution, the effect is unknown |

Allowed transitions:

| From | To |
| --- | --- |
| `queued_local` | `accepted_durable`, `failed`, `cancelled` |
| `accepted_durable` | `received_by_runner`, `failed`, `cancelled` |
| `received_by_runner` | `running`, `accepted_durable`, `failed`, `cancelled` |
| `running` | `awaiting_approval`, `completed`, `failed`, `cancelled`, `outcome_unknown` |
| `awaiting_approval` | `running`, `failed`, `cancelled`, `outcome_unknown` |
| `outcome_unknown` | `completed`, `failed` |

Rules of the lifecycle:

- `completed`, `failed`, and `cancelled` are terminal. They have no transitions.
- `received_by_runner` goes back to `accepted_durable` only after the lease of
  the runner expires. The runner did not record an execution intent, so a new
  runner can safely take the command.
- `outcome_unknown` never goes back to `running`. Wici does not repeat an
  uncertain effect automatically. Reconciliation or a user decides the outcome.
- An approval decision, approve or deny, returns the command to `running`.
- No state transitions to itself, and no state transitions to `queued_local`.

## 5. Delivery rules

- The client saves a command locally before upload and keeps its ID on retry.
- The server acknowledges a command only after the database commit.
- Deduplication uses tenant, command ID, and a payload fingerprint. The server
  rejects a known ID with a different payload.
- Each session has an ordered event stream. The server allocates positions with
  per-session serialization, not with timestamps or global sequences.
- A client reconnects with its last acknowledged position. If retained history
  does not cover that position, the server returns an explicit gap. Wici never
  skips data silently.
- State changes and outgoing events commit in one transaction (an outbox). A
  crash between commit and send is repaired by a resend from the outbox.
- Control messages (cancel, approval) have priority over output events.
- Display deltas are not durable. The protocol documents which events are.

## 6. Recovery rules

- After restart or reconnect, recover from durable state. Live notifications are
  only hints.
- Reconnect with exponential backoff, jitter, and a maximum delay.
- Leases and increasing fencing tokens protect execution ownership. Every write
  of a runner checks its token. A stale runner gets a rejection.
- Restart a workload only if its adapter declares safe recovery.
- Isolate corrupt records with diagnostics. Healthy sessions keep working.
- Bound all recovery attempts. If recovery needs a user, report a clear state.
- Recovery never deletes user data, resets identity or pairing, discards queued
  work, or bypasses revocation.

## 7. Security

- Use reviewed cryptographic libraries. Never write custom cryptography.
- Authorize every command, subscription, replay, artifact read, and approval.
- Support end-to-end encrypted payloads. The server routes them with metadata
  only. Document which metadata the server can read.
- Runners get scoped credentials, resource limits, and explicit file system and
  network permissions.
- Logs never contain prompts, payloads, tokens, or keys by default.

## 8. Performance

- The first deployment is one server process and one PostgreSQL database.
- One authenticated WebSocket connection carries many sessions.
- Queues, buffers, payloads, and retries have limits. Slow consumers get
  backpressure, not unbounded memory.
- Target: p95 below 200 ms from server acceptance to receipt by an online
  runner. This is a target. Measure it before you publish it.

## 9. Milestones

1. **Contract.** Command lifecycle, envelope, IDs, acknowledgements, failure
   model. The lifecycle is implemented. The other parts are open.
2. **Local storage.** SQLite outbox and inbox with crash and restart tests.
3. **Vertical slice.** Client, server with PostgreSQL, runner with a test agent.
   Live output, replay after restart, cancellation, unknown outcomes.
4. **Fault and security tests.** Process kills at each commit boundary, network
   faults, disk full, corrupt records, revocation, load limits.
5. **Public preview.** Approved license, protocol documentation, examples, and
   instructions for a one-server installation.

Later work: cloud runners and more than one server process. Plan this work only
after milestone 4 passes.

## 10. Verification

Every milestone needs evidence from tests:

- Kill client, server, and runner before and after each commit and send.
- Drop, duplicate, delay, and reorder messages.
- Force duplicate delivery and lost replies. Verify one dispatch or an explicit
  `outcome_unknown`.
- Return an old runner after its lease expires. Verify that its writes fail.
- Use real SQLite and PostgreSQL in storage tests, not only mocks.
- Use property tests for state machines, ordering, IDs, and decoders.

## 11. Open decisions

- License (needs owner approval).
- Envelope format and wire encoding.
- Encryption and key recovery model.
- FFI approach: hand-written C ABI or generated bindings.
- Retention defaults for commands, events, and deduplication records.
- First agent adapters. Assess the Agent Client Protocol before a custom one.
