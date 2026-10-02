# Wici project instructions

## Purpose and plan

Wici is an open-source Rust system that connects applications to AI agents on
local or remote machines. It delivers commands, resumable events, approvals, and
results through one protocol. Wici does not depend on one application, agent,
model vendor, or cloud provider.

Read [the project plan](docs/project-plan.md) before you change architecture or
behavior. The plan defines the scope, the command lifecycle, the delivery rules,
and the milestones. Never describe planned work as implemented or verified.

## Priorities

1. Preserve accepted work, durable data, authorization, and recovery data.
2. Recover from failures without a repeat of uncertain external actions.
3. Keep commands and streams responsive under load.
4. Keep the protocol general and the code readable and modular.
5. Make self-hosting and contribution straightforward.

Publish only guarantees that an explicit failure model and tests support. Never
claim unlimited uptime or exactly-once execution of arbitrary agent tools.

## Architecture

- Keep protocol types and state machines independent of storage, network, UI,
  agents, and cloud providers.
- Keep SQLite and PostgreSQL adapters separate from the shared semantics.
- Keep delivery, agent execution, and provisioning separate.
- Build the library and the standalone binaries from the same core.
- Keep the FFI layer thin. Never let a Rust panic cross the FFI boundary.
- Never run agents inside the UI process of a host application.
- Do not add application-specific operations to the protocol. Use adapters or
  versioned extensions.
- Add a crate or module only when real code needs it.

## Code quality

- Write code, documentation, errors, and examples in clear English.
  Reply to the user briefly in their language.
- Follow the Rust API Guidelines and the `rustfmt.toml` of this repository.
- One responsibility for each module and each function. Keep functions short.
- Do not duplicate logic. Before you write a function, search for an existing
  one and reuse or extend it. Put shared logic in one place.
- Prefer explicit types, enums, exhaustive `match`, and typed errors.
- Public errors keep actionable context and never contain secrets.
- No `unwrap`, `expect`, `panic!`, or unchecked indexing outside of tests.
- No `unsafe` outside of the FFI crate. Document each `unsafe` block.
- Suppress a lint only with `#[expect(lint, reason = "...")]`.
- Bound queues, tasks, buffers, payloads, retries, and concurrency. Use
  deadlines, cancellation, backpressure, and graceful shutdown. Never block an
  async executor with disk, CPU-heavy work, or process waits.
- Document each public item. Include failure behavior, and for async or storage
  code, cancellation and durability.
- Keep wire versions separate from crate versions. Never break old peers,
  persisted records, or migrations silently.
- Do not choose or change the license without owner approval.

## Tests

Every change ships with tests in the same commit. No code exists without tests.

- Unit tests: each function, each branch, and each error path.
- Assumption tests: each documented rule or invariant has a test that checks it.
- Property tests (`proptest`): state machines, ordering, IDs, and decoders.
- Integration tests: real SQLite and PostgreSQL for storage and recovery.
  Mocks never replace real storage and restart tests.
- Failure tests: protocol and recovery changes need fault injection at the
  relevant crash and restart boundaries.
- Line coverage of the workspace must stay at or above 90 percent.

## Reliability rules

- Persist before durable acceptance. Distinguish local save, server acceptance,
  runner receipt, execution, and completion.
- Retry with the original ID. Bind deduplication to tenant, operation, and
  payload.
- After reconnect or restart, reconcile from durable state. Notifications are
  only hints.
- Record uncertain outcomes explicitly. Never repeat an external effect only
  because a response was lost.
- Self-repair is bounded, observable, and non-destructive. Never erase data,
  regenerate identity, reset pairing, or bypass authorization.
- Keep evidence of corrupt records and isolate their failure.

## Verification

Run the full check before each commit:

```sh
scripts/verify.sh
```

The script runs formatting, Clippy, tests, documentation tests, documentation
build, dependency audit (`cargo-deny`), coverage (`cargo-llvm-cov`), and the
duplication check (`jscpd`). CI runs the same checks on Linux and macOS and also
checks the MSRV.

## Commits

- Commit each small step that works and passes `scripts/verify.sh`.
- Subject: imperative mood, at most 60 characters. Say what the application can
  do now, for example `Add command state transition rules`.
- Body (optional): at most 3 short lines with the reason or the limits.
- One logical change for each commit. Do not mix refactors with behavior.
- Do not commit secrets, credentials, build output, or local settings.

Preserve unrelated work. Do not publish a package, deploy infrastructure, or
change repository visibility without authorization. Report what changed, what
was verified, and the remaining limits. Do not add work logs to these files.
