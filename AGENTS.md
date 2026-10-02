# Wici rules

Wici connects paired devices: commands, streams, approvals, results, files.
Read [the plan](docs/project-plan.md) before changing behavior. Never
call planned work done.

## Writing

- English. Shortest wording that stays clear. Applies to docs, comments,
  errors, and commits.
- Reply to the user briefly, in their language.

## Priorities

1. Never lose accepted work, data, auth, or recovery state.
2. Never repeat an uncertain external action.
3. Stay responsive under load.
4. Keep the protocol generic and the code modular.
5. Keep self-hosting and contributing easy.

Claim only guarantees backed by a failure model and tests.

## Architecture

- Protocol and state machines know nothing of storage, network, UI, agents,
  or clouds.
- SQLite and PostgreSQL adapters stay separate from shared logic.
- Library and binaries share one core.
- Thin FFI. No panic crosses it.
- No app-specific operations in the protocol. Use adapters or extensions.
- New crate or module only when real code needs it.

## Code

- Rust API Guidelines and `rustfmt.toml`.
- One job per module and function. Short functions.
- No duplicated logic. Reuse or extend existing code first.
- Explicit types, enums, exhaustive `match`, typed errors.
- Errors keep context, never secrets.
- No `unwrap`, `expect`, `panic!`, or unchecked indexing outside tests.
- `unsafe` only in the FFI crate, each block documented.
- Suppress lints only with `#[expect(lint, reason = "...")]`.
- Bound queues, buffers, payloads, retries, concurrency. Use deadlines,
  cancellation, backpressure. Never block an async executor.
- Document public items, including failures. For async or storage code, also
  cancellation and durability.
- Wire versions are separate from crate versions. Never silently break peers,
  stored records, or migrations.
- License changes need owner approval.

## Tests

No code without tests, in the same commit.

- Unit: every function, branch, and error path.
- Assumptions: every documented rule has a test.
- Property (`proptest`): state machines, ordering, IDs, decoders.
- Storage and recovery: real SQLite and PostgreSQL, not mocks.
- Protocol and recovery changes: fault injection at crash and restart points.
- Line coverage ≥ 90%.

## Reliability

- Persist before acknowledging.
- Retry with the original ID. Deduplicate by pair, sender, ID, and payload.
- After restart, reconcile from durable state.
- Record uncertain outcomes. Never repeat an effect because a reply was lost.
- Self-repair is bounded and non-destructive. Never erase data, reset
  identity or pairing, or bypass auth.
- Quarantine corrupt records and keep the evidence.

## Verify

Run before every commit (CI runs the same, plus macOS and MSRV):

```sh
scripts/verify.sh
```

## Commits

- Small, working steps that pass `scripts/verify.sh`.
- Subject: imperative, ≤ 60 chars, what the app can do now.
- Body: optional, ≤ 3 short lines.
- One logical change per commit.
- No secrets, build output, or local settings.

Preserve unrelated work. No publishing, deploying, or visibility changes
without approval.
