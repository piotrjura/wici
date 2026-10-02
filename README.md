# Wici

Wici is an open-source Rust system that connects applications to AI agents on
other machines. It delivers commands, events, approvals, and results through
one protocol, and it recovers from network and process failures without losing
accepted work.

Status: early development. Only the command lifecycle in `wici-protocol` exists.
Read the [project plan](docs/project-plan.md) for the scope and the milestones.

## Requirements

- Rust: the toolchain in `rust-toolchain.toml` (rustup installs it). MSRV: 1.85.
- `cargo install --locked cargo-deny cargo-llvm-cov`
- Node.js, for the duplicate code check.

## Verify

```sh
scripts/verify.sh
```

The script runs the same checks as CI: formatting, Clippy, tests, documentation,
dependency audit, coverage (90 percent of lines minimum), and duplicate code.

## License

Not yet chosen. Do not redistribute the code before a license is published.
