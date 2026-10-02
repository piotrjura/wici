# Wici plan

Status: protocol, crypto, server, client, artifacts, C ABI, and Swift package
exist and have tests. Push hints and app integration are planned.

## Goal

Paired devices exchange commands, live streams, approvals, results, and files
in real time, end-to-end encrypted, through a relay server. Wici guarantees:

- No accepted command is lost.
- A retry never runs a command twice.
- Data is pushed instantly. No polling.
- A reconnecting device gets every durable event it missed.
- The true command state is always visible, even when it is unknown.
- The server never reads payloads.

## Terms

- **Device**: an app instance with its own identity key.
- **Pair**: two devices that trust each other. The unit of authorization.
- **Command**: request with a unique ID and a lifecycle.
- **Stream**: ordered events of one task, for example agent output.
- **Event**: durable stream record with a position.
- **Live update**: non-durable delta, for example a partial token.
- **Artifact**: encrypted file attached to a command or event.

## Crates

| Crate | Role |
| --- | --- |
| `wici-protocol` | IDs, frames, lifecycles, validation |
| `wici-crypto` | Device keys, pair keys, sealing |
| `wici-server` | Relay: auth, pairing, routing, storage (PostgreSQL) |
| `wici-client` | Device side: connect, outbox, inbox, replay (SQLite) |
| `wici-ffi` | C ABI for native apps |

Add a crate only when code needs it. Protocol and crypto know nothing of I/O.

## Devices and pairing

Any two devices can pair. No fixed roles.

1. Each device creates an Ed25519 identity key once. Its device ID is the
   public key. Wici never regenerates it silently.
2. Device A creates an invitation: pair ID, one-time token, A's keys, server
   URL, expiry (≤ 2 min). It travels out of band (QR code, link).
3. Device B claims it with the token and sends its keys.
4. A approves B. The pair becomes active.
5. Either device unpairs. The server revokes the pair, deletes its queued data
   and artifacts, and rejects further traffic.

Pair states: `invited` → `claimed` → `active` → `revoked`. `invited` and
`claimed` can also become `expired` or `revoked`.

Each side of a pair lists the operations it accepts. The app defines them.

## Encryption

- Pair key: X25519 agreement of both devices, HKDF-SHA256 with pair ID and
  invitation token. One key per direction.
- Payloads: ChaCha20-Poly1305, random nonce, routing header as AAD.
- Artifacts: random key per file, sent inside the encrypted message.
- Server sees only pair, sender, IDs, positions, sizes, and times.
- Every primitive is available in Swift CryptoKit.

## Transport

- One WebSocket per device. Device signs a server challenge to connect.
- Server pushes every frame on arrival.
- Frames carry commands, replies, events, live updates, acks, presence.
- Control frames (cancel, approval) go before output.
- Live updates are dropped under backpressure. Durable events never are.

## Delivery

- Save locally before sending. Retry with the same ID.
- Acknowledge only after the server commit.
- Deduplicate by pair + sender + ID + payload hash. Reject a reused ID with a
  new payload.
- Each stream has ordered positions, assigned per stream.
- Reconnect from the last acknowledged position. Missing history returns an
  explicit gap, never a silent skip.
- Commands carry a deadline. Expired commands fail and never run.

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
- `received_by_runner` → `accepted_durable` only after the receiver lease expires.
- `outcome_unknown` never runs again. Reconciliation or a user resolves it.
- Approve and deny both return to `running`.
- No self-transitions. Nothing returns to `queued_local`.

## Presence

The server knows which devices are connected and pushes online and offline
changes to paired devices. It stores last-seen time. Offline devices can get a
push notification hint with no content.

## Recovery

- Recover from durable state. Notifications are only hints.
- Reconnect at once on network change, else capped backoff with jitter.
- Quarantine corrupt records. Other streams keep working.
- Never delete data, reset identity or pairing, drop queued work, or bypass
  revocation.

## Limits

Every queue, buffer, payload, artifact, pair count, and rate is bounded and
configurable. Hitting a limit returns an explicit error.

## Build order

1. Protocol: IDs, pair lifecycle, frames, validation.
2. Crypto: device keys, pairing handshake, sealing.
3. Server: auth, pairing, commands, streams, replay, PostgreSQL.
4. Client: SQLite outbox and inbox, reconnect, replay, Rust API.
5. Artifacts: chunked encrypted upload and download.
6. Presence and push hints.
7. FFI and Swift package.

## Verification

- Kill each process before and after each commit and send.
- Drop, duplicate, delay, and reorder frames.
- Lost replies: one dispatch or explicit `outcome_unknown`.
- Revoked pair: every further frame fails.
- Real SQLite and PostgreSQL, not only mocks.
- Latency of push and replay is measured, not assumed.

License: not chosen yet.
