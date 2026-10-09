# Code map

Use this map to find code and tests. Public API docs define behavior.
The [plan](project-plan.md) separates existing behavior from planned work.
Tests inside source modules cover local rules. The paths below cover wider flows.

| Area | Entry points | Tests | Keep intact |
| --- | --- | --- | --- |
| Protocol | `crates/wici-protocol/src/{frame,body,command_state,pair_state}.rs` | Tests in these modules | Wire compatibility and legal transitions |
| Crypto and pairing | `crates/wici-crypto/src/{invitation,pair,device,vault}.rs` | Tests in these modules | Identity, secrets, authentication |
| Server routing | `crates/wici-server/src/{handlers,session,delivery,hub}.rs` | `crates/wici-server/tests/server/{ws,client}.rs` | Durable delivery and bounded queues |
| Server storage | `crates/wici-server/src/store/` | `crates/wici-server/tests/server/store_*.rs` | Shared SQLite and PostgreSQL rules |
| Client and recovery | `crates/wici-client/src/{lib,runtime,inbound,outbound,db}.rs` | `crates/wici-client/tests/client/{flows,keepalive,shutdown}.rs` and module tests | Durable state, original IDs, uncertain outcomes |
| Files | `crates/wici-client/src/transfer.rs`, `crates/wici-crypto/src/artifact.rs`, `crates/wici-server/src/store/artifacts.rs` | Module tests and `store_artifacts.rs` in server tests | Encryption, resumable uploads, limits |
| C ABI | `crates/wici-ffi/src/{lib,rpc,config}.rs`, `include/wici.h` in that crate | `crates/wici-ffi/tests/abi.rs`, `src/shutdown_tests.rs` | No panic across FFI, safe callback shutdown |
| Swift | `swift/Sources/WiciKit/WiciClient.swift` | `swift/Tests/WiciKitTests/` | Bounded events, cancellation, recovery |
| Load test | `crates/wici-load/src/{lib,user,payload}.rs` | `crates/wici-load/tests/load.rs` | Lost and duplicate message detection |
| Test helpers | `crates/wici-testkit/src/lib.rs` | Module tests and client/server suites | Real temporary databases and servers |
| Verification | `scripts/verify*.sh`, `.github/workflows/ci.yml` | `scripts/test-tool-path.sh`, `scripts/test-verify-merge.sh` | Same checks locally and in CI |

## Verification

- `scripts/verify.sh`: Rust format, lints, tests, docs, dependencies, coverage, duplicates.
- `scripts/verify-msrv.sh`: workspace check with the declared minimum Rust version.
- `scripts/verify-swift.sh`: macOS XCFramework and Swift tests against a real server.
- `scripts/verify-merge.sh`: all local checks, including all Apple targets. Requires macOS.

Cross-platform CI remains required. Passing checks do not prove that every failure mode is covered.
