-- Sealed artifacts, stored as ordered chunks.
CREATE TABLE artifacts (
    pair_id UUID NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
    id UUID NOT NULL,
    uploader BYTEA NOT NULL,
    total BIGINT NOT NULL CHECK (total > 0),
    hash BYTEA NOT NULL CHECK (octet_length(hash) = 32),
    received BIGINT NOT NULL DEFAULT 0,
    complete BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, id),
    CHECK (received >= 0 AND received <= total),
    CHECK (NOT complete OR received = total)
);
CREATE INDEX artifacts_created ON artifacts (created_at);

CREATE TABLE artifact_chunks (
    pair_id UUID NOT NULL,
    artifact_id UUID NOT NULL,
    start BIGINT NOT NULL CHECK (start >= 0),
    data BYTEA NOT NULL CHECK (octet_length(data) > 0),
    PRIMARY KEY (pair_id, artifact_id, start),
    FOREIGN KEY (pair_id, artifact_id) REFERENCES artifacts (pair_id, id) ON DELETE CASCADE
);
