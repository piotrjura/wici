# Wici

Rust system that connects apps to AI agents on other machines. Delivers
commands, events, approvals, and results, and survives network and process
failures without losing accepted work.

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
