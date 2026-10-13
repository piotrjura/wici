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

Build and verification scripts find Cargo and Homebrew tools in GUI Git
clients too. If Node is missing from PATH, they try the nvm default.

## Run the server

On one SQLite file, with no database server:

```sh
WICI_DATABASE_URL=sqlite:wici.db cargo run --release -p wici-server
```

For more than one server process, use PostgreSQL:
`WICI_DATABASE_URL=postgres://user@host/db`. `WICI_LISTEN` sets the address
(default `127.0.0.1:8080`).

TLS clients select the `ring` crypto provider and trust native plus Mozilla
root certificates. Mozilla roots support iOS without a Unix certificate store.
Certificate and hostname checks remain enabled. Swift packages bundle the
trust-anchor license in `ThirdPartyNotices.txt`.

### Run with Docker and TLS

The server has no TLS. Clients check certificates, so the server must have a
public certificate. Caddy gets one from Let's Encrypt and renews it.

Before you start, make sure that:

- a DNS record points your domain, for example `relay.example.com`, to the host
- ports 80 and 443 are open on the host

Run the server and Caddy on one Docker network:

```sh
docker build -t wici-server .
docker network create wici
docker run -d --name wici --restart unless-stopped --network wici \
  -v wici-data:/data wici-server
docker run -d --name caddy --restart unless-stopped --network wici \
  -p 80:80 -p 443:443 -v caddy-data:/data caddy:2 \
  caddy reverse-proxy --from relay.example.com --to wici:8080
```

Check it: `curl https://relay.example.com/health` prints `ok`.
Clients use `wss://relay.example.com/v1/ws`.

The `wici` container publishes no port, so plain traffic stays inside the
Docker network. The `caddy-data` volume keeps the certificates. The image
runs as a non-root user. A bind mount for `/data` must be writable by
UID 65532. For more than one replica, use PostgreSQL. Use PostgreSQL only in a
private network, because the server connects to it without TLS.

## Verify

```sh
scripts/verify.sh
```

Format, Clippy, tests, docs, audit, coverage ≥ 90%, duplicates. Same as CI.
`scripts/verify-swift.sh` builds the XCFramework and runs the Swift tests.

Before merge, run `scripts/verify-merge.sh` on macOS. It also checks MSRV
and builds all Apple targets. Install the MSRV toolchain and Rust targets
listed in `.github/workflows/ci.yml` first. Required Linux and macOS CI
must also pass. See the [code map](docs/code-map.md) for modules and tests.

## Load test

```sh
scripts/load.sh --users 2500 --active 250 --seconds 120
```

Runs a release server on a temporary PostgreSQL. Simulated Mac and phone
pairs send notice, request, and 8 KB snapshot messages. Reports latencies
and server and database CPU and memory. Fails on lost messages, errors, or
dropped connections. `--help` lists the options.

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
