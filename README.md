# Wici

Rust system that connects paired devices in real time, end-to-end encrypted.
Commands, streams, approvals, results, and files survive failures.

Status: early. Server and Rust client work; see the [plan](docs/project-plan.md).

## Setup

- Rust from `rust-toolchain.toml` (MSRV 1.85)
- `cargo install --locked cargo-deny cargo-llvm-cov`
- Node.js (duplicate check)
- PostgreSQL binaries (`initdb`, `pg_ctl`) for tests
- Xcode, for the Swift package

## Verify

```sh
scripts/verify.sh
```

Format, Clippy, tests, docs, audit, coverage ≥ 90%, duplicates. Same as CI.
`scripts/verify-swift.sh` builds the XCFramework and runs the Swift tests.

## Use from Swift

`swift/` is the `WiciKit` package. Build `swift/build/WiciFFI.xcframework`
with `scripts/build-xcframework.sh --all`, then add the package.

Consume `client.events` with `for try await`. `eventBufferCapacity` defaults
to 256. Live updates may drop when full. Other overflow ends the stream with
`event_overflow`; queued events drain first. Close, reopen with the same
secret/database, read `pending` and current state, then resume. Mark a message
`handled` only after processing it.

Call `close()` off the main thread. It waits for callbacks and resolves
pending calls once. Interrupted calls report `outcome_unknown`: a write may
have committed. Reconcile durable state before retrying; do not submit the
same effect with a fresh ID. Task cancellation does not undo accepted work.

The Swift package has transport tests. Sfora Mac/iOS integration and a
terminal monitor are planned. The real project list and simulator streaming
have not been tested.

## License

Not chosen yet.
