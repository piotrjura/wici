-- Same model as the PostgreSQL schema. Times are milliseconds since the
-- Unix epoch. IDs are 16-byte UUID blobs.

-- Devices are identified by their Ed25519 public key.
CREATE TABLE devices (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    last_seen_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
) STRICT;

CREATE TABLE pairs (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    state TEXT NOT NULL CHECK (state IN ('invited', 'claimed', 'active', 'revoked', 'expired')),
    inviter BLOB NOT NULL REFERENCES devices (id),
    invitee BLOB REFERENCES devices (id),
    claim_hash BLOB NOT NULL CHECK (length(claim_hash) = 32),
    greeting BLOB,
    -- Deadline while invited or claimed. NULL once active.
    expires_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    CHECK (invitee IS NULL OR invitee <> inviter),
    CHECK (state IN ('invited', 'expired') OR invitee IS NOT NULL),
    CHECK (state <> 'active' OR expires_at IS NULL)
) STRICT;
CREATE INDEX pairs_inviter ON pairs (inviter);
CREATE INDEX pairs_invitee ON pairs (invitee);
CREATE INDEX pairs_pending_expiry ON pairs (expires_at) WHERE state IN ('invited', 'claimed');

-- One row per recipient and lane of an active pair. Serializes positions.
CREATE TABLE lanes (
    pair_id BLOB NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
    recipient BLOB NOT NULL REFERENCES devices (id),
    lane TEXT NOT NULL CHECK (lane IN ('control', 'data')),
    next_position INTEGER NOT NULL DEFAULT 1 CHECK (next_position >= 1),
    acked_position INTEGER NOT NULL DEFAULT 0 CHECK (acked_position >= 0),
    PRIMARY KEY (pair_id, recipient, lane),
    CHECK (acked_position < next_position)
) STRICT;
-- Finds a device's lanes without a full scan. Every delivery round needs it.
CREATE INDEX lanes_recipient ON lanes (recipient);

-- Durable messages. `sealed` is cleared after the recipient acks; the row
-- stays for deduplication.
CREATE TABLE messages (
    pair_id BLOB NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
    recipient BLOB NOT NULL,
    lane TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 1),
    id BLOB NOT NULL CHECK (length(id) = 16),
    sender BLOB NOT NULL,
    payload_hash BLOB NOT NULL CHECK (length(payload_hash) = 32),
    sealed BLOB,
    accepted_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    PRIMARY KEY (pair_id, recipient, lane, position),
    UNIQUE (pair_id, sender, id),
    FOREIGN KEY (pair_id, recipient, lane) REFERENCES lanes (pair_id, recipient, lane) ON DELETE CASCADE
) STRICT;
CREATE INDEX messages_pending ON messages (pair_id, recipient, lane, position) WHERE sealed IS NOT NULL;

-- Sealed artifacts, stored as ordered chunks.
CREATE TABLE artifacts (
    pair_id BLOB NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
    id BLOB NOT NULL CHECK (length(id) = 16),
    uploader BLOB NOT NULL,
    total INTEGER NOT NULL CHECK (total > 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    received INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    PRIMARY KEY (pair_id, id),
    CHECK (received >= 0 AND received <= total),
    CHECK (NOT complete OR received = total)
) STRICT;
CREATE INDEX artifacts_created ON artifacts (created_at);

CREATE TABLE artifact_chunks (
    pair_id BLOB NOT NULL,
    artifact_id BLOB NOT NULL,
    start INTEGER NOT NULL CHECK (start >= 0),
    data BLOB NOT NULL CHECK (length(data) > 0),
    PRIMARY KEY (pair_id, artifact_id, start),
    FOREIGN KEY (pair_id, artifact_id) REFERENCES artifacts (pair_id, id) ON DELETE CASCADE
) STRICT;
