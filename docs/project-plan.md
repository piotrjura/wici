# Wici plan

Status: protocol, crypto, server, client, artifacts, presence, C ABI, Swift
package, and load test exist and have tests. See [Build order](#build-order)
for planned work.

## Goal

Paired devices exchange commands, live streams, approvals, results, and files
in real time, end-to-end encrypted, through a relay server. Wici guarantees:

- No accepted command is lost.
- A retry never runs a command twice.
- Data is pushed instantly. No polling.
- A reconnecting device gets every durable message it missed.
- The true command state is always visible, even when it is unknown.
- The server never reads payloads.

## Terms

- **Device**: an app instance with its own identity key.
- **Pair**: two devices that trust each other. The unit of authorization.
- **Lane**: ordered channel of durable messages, `control` or `data`.
- **Message**: durable, sealed body with an ID and a lane position.
- **Command**: message that asks the peer to run an operation. Has a lifecycle.
- **Stream**: events of one task, for example agent output.
- **Live update**: non-durable delta, for example a partial token.
- **Artifact**: encrypted file. Its key travels in a sealed message.

## Crates

| Crate | Role |
| --- | --- |
| `wici-protocol` | IDs, frames, bodies, lifecycles, validation |
| `wici-crypto` | Device keys, pairing, sealing, local vault, artifacts |
| `wici-server` | Relay: auth, pairing, routing, storage (PostgreSQL or SQLite) |
| `wici-client` | Device side: outbox, inbox, reconnect (SQLite) |
| `wici-ffi` | C ABI for native apps: JSON requests and events |
| `wici-load` | Load test with simulated device pairs |
| `wici-testkit` | Test helpers: temporary databases and servers |

`swift/` holds `WiciKit`, a Swift package over the C ABI.

Add a crate only when code needs it. Protocol and crypto know nothing of I/O.

Server storage rules are shared and call an adapter trait. Each database is
one adapter, and both pass the same tests:

- PostgreSQL: many server processes on one database. Row locks order writers.
- SQLite: one server process, one file. One writer connection orders writes,
  read-only connections serve reads. WAL with full sync.

## Devices and pairing

Any two devices can pair. No fixed roles.

1. Each device creates its keys once: Ed25519 identity and X25519 agreement.
   The device ID is the Ed25519 public key. Wici never regenerates it silently.
2. Device A creates an invitation: pair ID, random token, A's keys, server
   URL, and expiry (2 min). It travels out of band as a `wici:` link or QR code.
   A sends only the claim hash to the server. The token never reaches it.
3. Device B claims with the claim secret. It sends a greeting with its keys,
   sealed with a token key.
4. A checks the greeting and approves B within 10 min. The pair becomes
   active. A forged greeting makes A unpair.
5. Either device unpairs, also offline. The server revokes the pair, deletes
   its queued messages and artifacts, and rejects further traffic.

Pair states: `invited` → `claimed` → `active` → `revoked`. `invited` and
`claimed` can also become `expired` or `revoked`. A device has at most 16 open
pairs.

Planned: each side lists the operations it accepts. The app defines them.

## Encryption

- Pair keys: X25519 of both devices, HKDF-SHA256 with the token as salt, and
  pair ID, sender, and receiver as info. One key per direction.
- Messages: ChaCha20-Poly1305, random nonce. AAD binds pair, sender, message
  ID, and lane. Live updates bind pair and sender.
- Artifacts: random key per file, 64 KiB chunks, nonce from the chunk index.
  AAD binds artifact ID, index, and a final-chunk flag.
- Local vault: a key from the device secret seals pair keys and received
  bodies in SQLite. Only the device secret goes in a secure store (Keychain).
- Server sees only pair, sender, IDs, lanes, positions, sizes, and times.
- Every primitive is available in Swift CryptoKit.

## Transport

- One WebSocket per device at `/v1/ws`. A new connection replaces the old one.
  `/health` answers `ok`.
- JSON text frames tagged by `type`. Wire version 1. Unknown fields are
  ignored.
- The device signs a server challenge within 10 s to connect.
- Server pushes every frame on arrival. Pings every 20 s. A connection with no
  frame for 60 s closes.
- Clients ping every 15 s. A client reconnects after 45 s with no message
  from the server.
- A replaced connection does not announce the device offline.
- Frames carry pairing, messages, acks, live updates, presence, artifacts,
  and errors.
- Each lane has its own order, so output never delays control.
- Live updates drop when the peer is offline or its data queue is full. A full
  control queue closes the connection. Durable messages never drop.
- Rate limit per connection: 100 frames/s, burst 200.

## Message bodies

All bodies are sealed. Apps define operations, inputs, and data.

| Body | Content |
| --- | --- |
| `command` | Operation, input, deadline, optional stream |
| `status` | New command state and optional output |
| `cancel` | Command ID |
| `approval_request` | Command ID, approval ID, details |
| `approval_decision` | Command ID, approval ID, allow |
| `event` | Stream ID and data |

A live update carries a stream ID and data.

## Delivery

- The client saves a message in its outbox before it sends. Retries resend
  the same ID and sealed bytes.
- The server acknowledges only after the database commit.
- The server deduplicates by pair + sender + ID + payload hash. A reused ID
  with a new payload or lane returns `conflict`.
- Positions are per pair, recipient, and lane. They start at 1 and have no
  gaps.
- The recipient acks a position only after its SQLite commit. On a gap it does
  not ack, so the server resends in order.
- The server keeps a message until the recipient acks it. Then it clears the
  payload and keeps the row for deduplication.
- On reconnect, the server pushes every unacked message. There is no history
  replay of acked messages.
- At most 128 unacked messages are in flight per lane. A lane with 4096
  pending messages rejects new ones with `limit_exceeded`.
- Commands carry a deadline. The receiver marks an expired command `failed`
  and never gives it to the app.
- The app marks a received message `handled` after it processes it.
  `pending` lists the rest.

## Command lifecycle

| State | Meaning |
| --- | --- |
| `queued_local` | Saved on the sender |
| `accepted_durable` | Committed by the server |
| `received_by_runner` | Held by the receiver, not started |
| `running` | Execution intent recorded, started |
| `awaiting_approval` | Waits for an approval |
| `completed` | Done, result saved |
| `failed` | Rejected, expired, or errored |
| `cancelled` | Cancelled before completion |
| `outcome_unknown` | Receiver died mid-run, effect unknown |

| From | To |
| --- | --- |
| `queued_local` | `accepted_durable`, `failed`, `cancelled` |
| `accepted_durable` | `received_by_runner`, `failed`, `cancelled` |
| `received_by_runner` | `running`, `accepted_durable`, `failed`, `cancelled` |
| `running` | `awaiting_approval`, `completed`, `failed`, `cancelled`, `outcome_unknown` |
| `awaiting_approval` | `running`, `failed`, `cancelled`, `outcome_unknown` |
| `outcome_unknown` | `completed`, `failed` |

- Terminal: `completed`, `failed`, `cancelled`.
- The receiver reports states with `status`. It never reports `queued_local`
  or `accepted_durable`.
- A status report proves acceptance, so a lost `accepted` is skipped.
- On open, the receiver marks its `running` and `awaiting_approval` commands
  `outcome_unknown` and tells the peer.
- `outcome_unknown` never runs again. Reconciliation or a user resolves it.
- Approve and deny both return to `running`.
- No self-transitions. Nothing returns to `queued_local`.
- Planned: receiver lease. `received_by_runner` → `accepted_durable` only
  after the lease expires.

## Artifacts

- The client seals the file, uploads chunks in order, and sends an
  `ArtifactRef` (ID, key, sizes, hash, media type) in a sealed message.
- A repeated chunk is ignored, so an upload resumes. A gap is rejected.
- The server checks the SHA-256 of the sealed bytes at the end. A mismatch
  deletes the upload.
- The peer downloads chunk by chunk and checks the hash.
- Either device deletes an artifact. A repeated delete does nothing.
- Limits: 64 MiB sealed, 256 KiB per chunk frame, 16 unfinished uploads per
  pair.
- Retention: unfinished uploads go after 24 h, finished artifacts after 7 days.

## Presence

The server pushes online and offline changes to paired devices with the
last-seen time.

Planned: push notification hints with no content for offline devices.

## Recovery

- Recover from durable state. Notifications are only hints.
- Reconnect at once after `reconnect_now` (for example on network change).
  Else backoff from 200 ms to 30 s with jitter.
- The client quarantines a message that it cannot open. It keeps the sealed
  bytes and the reason. Other lanes and pairs keep working.
- Never delete data, reset identity or pairing, drop queued work, or bypass
  revocation.

## Native apps

- The C ABI takes JSON requests and sends JSON events through a callback. No
  panic crosses it.
- `WiciKit` exposes the client as async calls and an `events` sequence with a
  bounded buffer (default 256).
- When the buffer is full, live updates drop. Other overflow ends the sequence
  with `event_overflow`. The app reopens and reads `pending`.
- A call that a close interrupts reports `outcome_unknown`.

## Limits

Every queue, buffer, payload, artifact, pair count, and rate is bounded.
Hitting a limit returns an explicit error. Library users set limits in
`Limits`, `Timeouts`, and `ClientConfig`. The server binary reads only
`WICI_DATABASE_URL` (`postgres://...` or `sqlite:path`), `WICI_LISTEN`, and
`WICI_DATABASE_CONNECTIONS`.

## Build order

Done:

1. Protocol: IDs, pair lifecycle, frames, bodies, validation.
2. Crypto: device keys, pairing handshake, sealing, local vault.
3. Server: auth, pairing, lanes, delivery, live updates, PostgreSQL.
4. Client: SQLite outbox and inbox, reconnect, recovery, Rust API.
5. Artifacts: chunked encrypted upload and download.
6. Presence.
7. C ABI and Swift package.
8. Load test.
9. SQLite server adapter: run the relay from one file, no database server.

Next:

1. Push hints.
2. Operation lists per pair side.
3. Receiver lease.
4. Fault injection: frames and crash points.
5. App integration: Sfora Mac and iOS, terminal monitor.

## Verification

Exists:

- Real SQLite and PostgreSQL tests, not mocks.
- Client and server restarts with queued work.
- Lost replies: an interrupted command becomes `outcome_unknown`.
- Revoked pair: every further frame fails.
- `scripts/load.sh` measures push latency and fails on lost or duplicated
  messages, errors, or dropped connections.

Planned:

- Kill each process before and after each commit and send.
- Drop, duplicate, delay, and reorder frames.

License: not chosen yet.
