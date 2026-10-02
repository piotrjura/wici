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

## License

Not chosen yet.
