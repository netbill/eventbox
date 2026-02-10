CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE outbox_events (
    event_id UUID   PRIMARY KEY NOT NULL,
    seq      BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE CHECK ( seq >= 0 ),

    topic    VARCHAR NOT NULL,
    key      VARCHAR NOT NULL,
    type     VARCHAR NOT NULL,
    version  INT     NOT NULL,
    producer VARCHAR NOT NULL,
    payload  JSONB   NOT NULL,

    reserved_by     VARCHAR,

    status          VARCHAR NOT NULL DEFAULT 'pending', -- pending | processing | sent | failed
    attempts        INT NOT NULL DEFAULT 0 CHECK ( attempts >= 0 ),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    last_attempt_at TIMESTAMPTZ,
    last_error      VARCHAR,

    sent_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),

    CHECK (status IN ('pending','processing','sent','failed'))
);

CREATE INDEX outbox_events_pending_ready_idx
    ON outbox_events (next_attempt_at, seq)
    WHERE status = 'pending';

CREATE INDEX outbox_events_key_idx
    ON outbox_events (key);

CREATE INDEX outbox_events_type_idx
    ON outbox_events (type);