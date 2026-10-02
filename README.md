# Wici

Rust system that connects paired devices in real time, end-to-end encrypted.
Commands, streams, approvals, results, and files survive failures.

Status: early. Only the command lifecycle exists. See the [plan](docs/project-plan.md).

## Setup

- Rust from `rust-toolchain.toml` (MSRV 1.85)
- `cargo install --locked cargo-deny cargo-llvm-cov`
- Node.js (duplicate check)

## Verify

```sh
scripts/verify.sh
```

Format, Clippy, tests, docs, audit, coverage ≥ 90%, duplicates. Same as CI.

## License

Not chosen yet.
