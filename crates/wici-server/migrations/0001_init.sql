-- Devices are identified by their Ed25519 public key.
CREATE TABLE devices (
    id BYTEA PRIMARY KEY CHECK (octet_length(id) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pairs (
    id UUID PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('invited', 'claimed', 'active', 'revoked', 'expired')),
    inviter BYTEA NOT NULL REFERENCES devices (id),
    invitee BYTEA REFERENCES devices (id),
    claim_hash BYTEA NOT NULL CHECK (octet_length(claim_hash) = 32),
    greeting BYTEA,
    -- Deadline while invited or claimed. NULL once active.
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (invitee IS NULL OR invitee <> inviter),
    CHECK (state IN ('invited', 'expired') OR invitee IS NOT NULL),
    CHECK (state <> 'active' OR expires_at IS NULL)
);
CREATE INDEX pairs_inviter ON pairs (inviter);
CREATE INDEX pairs_invitee ON pairs (invitee);
CREATE INDEX pairs_pending_expiry ON pairs (expires_at) WHERE state IN ('invited', 'claimed');

-- One row per recipient and lane of an active pair. Serializes positions.
CREATE TABLE lanes (
    pair_id UUID NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
    recipient BYTEA NOT NULL REFERENCES devices (id),
    lane TEXT NOT NULL CHECK (lane IN ('control', 'data')),
    next_position BIGINT NOT NULL DEFAULT 1 CHECK (next_position >= 1),
    acked_position BIGINT NOT NULL DEFAULT 0 CHECK (acked_position >= 0),
    PRIMARY KEY (pair_id, recipient, lane),
    CHECK (acked_position < next_position)
);

-- Durable messages. `sealed` is cleared after the recipient acks; the row
-- stays for deduplication.
CREATE TABLE messages (
    pair_id UUID NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
    recipient BYTEA NOT NULL,
    lane TEXT NOT NULL,
    position BIGINT NOT NULL CHECK (position >= 1),
    id UUID NOT NULL,
    sender BYTEA NOT NULL,
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    sealed BYTEA,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, recipient, lane, position),
    UNIQUE (pair_id, sender, id),
    FOREIGN KEY (pair_id, recipient, lane) REFERENCES lanes (pair_id, recipient, lane) ON DELETE CASCADE
);
CREATE INDEX messages_pending ON messages (pair_id, recipient, lane, position) WHERE sealed IS NOT NULL;
