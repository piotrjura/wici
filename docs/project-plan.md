# Wici plan

Status: early. Only the command lifecycle (`wici-protocol`) exists.

## Goal

Apps send commands to AI agents on other machines and get back events,
approval requests, and results. Wici guarantees:

- No accepted command is lost.
- A retry never runs a command twice.
- A reconnecting client gets every durable event it missed.
- Risky agent actions wait for approval.
- The true command state is always visible, even when it is unknown.

Not a sync engine, model framework, or workflow platform. No vendor lock-in.

## Terms

- **Tenant**: isolated owner of all data below.
- **Client**: app that sends commands and reads events.
- **Runner**: process that runs commands with an agent.
- **Session**: ordered stream of commands and events.
- **Command**: client request with a unique ID.
- **Event**: session record with an ordered position.
- **Approval**: client decision that lets an agent continue.
- **Artifact**: runner file, stored with a checksum.

## Crates

Add a crate only when code needs it. Dependencies point inward.

| Crate | Role | Status |
| --- | --- | --- |
| `wici-protocol` | Wire types, lifecycle | Started |
| `wici-core` | Delivery and recovery logic, storage traits | Planned |
| `wici-store-sqlite` | Local outbox, inbox, cursors | Planned |
| `wici-store-postgres` | Server storage | Planned |
| `wici-client` | Connect, subscribe, replay | Planned |
| `wici-server` | Auth, routing, acceptance | Planned |
| `wici-runner` | Agent adapters, execution records | Planned |
| `wici-ffi` | C ABI for native apps | Planned |

Agents never run in a host app's UI process.

## Command lifecycle

| State | Meaning |
| --- | --- |
| `queued_local` | Saved on the client |
| `accepted_durable` | Committed by the server |
| `received_by_runner` | Held by a runner, not started |
| `running` | Execution intent recorded, started |
| `awaiting_approval` | Waits for an approval |
| `completed` | Done, result saved |
| `failed` | Rejected, expired, or errored |
| `cancelled` | Cancelled before completion |
| `outcome_unknown` | Runner died mid-run, effect unknown |

| From | To |
| --- | --- |
| `queued_local` | `accepted_durable`, `failed`, `cancelled` |
| `accepted_durable` | `received_by_runner`, `failed`, `cancelled` |
| `received_by_runner` | `running`, `accepted_durable`, `failed`, `cancelled` |
| `running` | `awaiting_approval`, `completed`, `failed`, `cancelled`, `outcome_unknown` |
| `awaiting_approval` | `running`, `failed`, `cancelled`, `outcome_unknown` |
| `outcome_unknown` | `completed`, `failed` |

- Terminal: `completed`, `failed`, `cancelled`.
- `received_by_runner` → `accepted_durable` only after the runner lease expires.
- `outcome_unknown` never runs again. Reconciliation or a user resolves it.
- Approve and deny both return to `running`.
- No self-transitions. Nothing returns to `queued_local`.

## Delivery

- Save locally before upload. Retry with the same ID.
- Acknowledge only after the server commit.
- Deduplicate by tenant + ID + payload hash. Reject a reused ID with a new payload.
- Each session has ordered positions, assigned per session.
- Reconnect from the last acknowledged position. Missing history returns an
  explicit gap, never a silent skip.
- Commit state and outgoing events together (outbox).
- Cancel and approvals jump ahead of output.
- Display deltas are not durable.

## Recovery

- Recover from durable state. Notifications are only hints.
- Reconnect with capped exponential backoff and jitter.
- Leases with fencing tokens. Stale runners get rejected writes.
- Quarantine corrupt records. Other sessions keep working.
- Bounded retries. Never delete data, reset identity or pairing, drop queued
  work, or bypass revocation.

## Security

- Reviewed crypto libraries only.
- Authorize every command, subscription, replay, artifact read, and approval.
- Optional end-to-end encrypted payloads. Document visible metadata.
- No prompts, payloads, tokens, or keys in logs.

## Performance

- One server process and one PostgreSQL database.
- One WebSocket carries many sessions.
- Every queue, buffer, payload, and retry is bounded.
- Target, not measured: p95 < 200 ms from acceptance to runner receipt.

## Milestones

1. **Contract**: lifecycle (done), envelope, IDs, acks, failure model.
2. **Local storage**: SQLite outbox and inbox with crash tests.
3. **Vertical slice**: client → server (PostgreSQL) → runner → client, with
   replay, cancel, and unknown outcomes.
4. **Fault and security tests**: kills at every commit boundary, network
   faults, disk full, corrupt records, revocation, load.
5. **Public preview**: license, protocol docs, examples, one-server setup.

## Verification

- Kill each process before and after each commit and send.
- Drop, duplicate, delay, and reorder messages.
- Lost replies: one dispatch or explicit `outcome_unknown`.
- Expired lease: old runner writes fail.
- Real SQLite and PostgreSQL, not only mocks.

## Open decisions

License, wire encoding, encryption and key recovery, FFI approach, retention,
first agent adapters (check Agent Client Protocol first).
