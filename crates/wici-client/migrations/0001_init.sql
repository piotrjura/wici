-- Pairs known to this device. Secrets are sealed with the device vault.
CREATE TABLE pairs (
    id BLOB PRIMARY KEY,
    role TEXT NOT NULL CHECK (role IN ('inviter', 'invitee')),
    state TEXT NOT NULL CHECK (state IN ('invited', 'claimed', 'active', 'revoked', 'expired')),
    -- Operation to send to the server until it confirms.
    pending TEXT CHECK (pending IN ('invite', 'claim', 'approve', 'unpair')),
    peer BLOB,
    invitation BLOB NOT NULL,
    keys BLOB,
    updated_at INTEGER NOT NULL
);

-- Durable messages not yet accepted by the server. The sealed bytes are
-- resent unchanged, so the server can deduplicate them.
CREATE TABLE outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    pair BLOB NOT NULL,
    id BLOB NOT NULL,
    lane TEXT NOT NULL CHECK (lane IN ('control', 'data')),
    sealed BLOB NOT NULL,
    retry_at INTEGER NOT NULL DEFAULT 0,
    UNIQUE (pair, id)
);

-- Last saved position per received lane.
CREATE TABLE cursors (
    pair BLOB NOT NULL,
    lane TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (pair, lane)
);

-- Received messages. Bodies are sealed with the device vault.
CREATE TABLE inbox (
    pair BLOB NOT NULL,
    lane TEXT NOT NULL,
    position INTEGER NOT NULL,
    id BLOB NOT NULL,
    body BLOB NOT NULL,
    accepted_at INTEGER NOT NULL,
    handled INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pair, lane, position)
);
CREATE INDEX inbox_unhandled ON inbox (handled) WHERE handled = 0;

-- Messages that could not be opened. Kept as evidence.
CREATE TABLE quarantine (
    pair BLOB NOT NULL,
    lane TEXT NOT NULL,
    position INTEGER NOT NULL,
    id BLOB NOT NULL,
    sealed BLOB NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (pair, lane, position)
);

-- Lifecycle of commands this device sent or received.
CREATE TABLE commands (
    pair BLOB NOT NULL,
    id BLOB NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('outgoing', 'incoming')),
    state TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (pair, id, direction)
);
