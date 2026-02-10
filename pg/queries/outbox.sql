-- name: InsertOutboxEvent :one
INSERT INTO outbox_events (
    event_id, topic, key, type, version, producer, payload,
    status, attempts, next_attempt_at
) VALUES (
    sqlc.arg(event_id),
    sqlc.arg(topic),
    sqlc.arg(key),
    sqlc.arg(type),
    sqlc.arg(version),
    sqlc.arg(producer),
    sqlc.arg(payload)::jsonb,
    sqlc.arg(status),
    sqlc.arg(attempts),
    sqlc.arg(next_attempt_at)
)
ON CONFLICT (event_id) DO NOTHING
RETURNING *;

-- name: GetOutboxEventByID :one
SELECT *
FROM outbox_events
WHERE event_id = $1;

-- name: ReserveOutboxEvents :many
WITH ready AS (
    SELECT *
    FROM outbox_events
    WHERE status = 'pending'
      AND reserved_by IS NULL
      AND next_attempt_at <= (now() AT TIME ZONE 'UTC')
    ORDER BY seq
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(sort_limit)
),
key_heads AS (
    SELECT DISTINCT ON (r.topic, r.key)
        r.topic,
        r.key,
        r.seq AS head_seq
    FROM ready r
    WHERE NOT EXISTS (
        SELECT 1
        FROM outbox_events p
        WHERE p.topic = r.topic
            AND p.key = r.key
            AND p.status = 'processing'
            AND p.reserved_by IS NOT NULL
    )
    ORDER BY r.topic, r.key, r.seq
),
picked AS (
    SELECT e.event_id
    FROM outbox_events e
    JOIN key_heads k ON k.topic = e.topic AND k.key = e.key
    WHERE e.status = 'pending'
        AND e.reserved_by IS NULL
        AND e.next_attempt_at <= (now() AT TIME ZONE 'UTC')
    ORDER BY k.head_seq, e.seq
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_limit)
)
UPDATE outbox_events e
SET reserved_by = sqlc.arg(process_id),
    status      = 'processing'
WHERE e.event_id IN (SELECT event_id FROM picked)
RETURNING *;

-- name: MarkOutboxEventsAsSent :exec
WITH inp AS (
    SELECT i.event_id, s.sent_at
    FROM unnest(sqlc.arg(event_ids)::uuid[]) WITH ORDINALITY AS i(event_id, ord)
    JOIN unnest(sqlc.arg(sent_ats)::timestamptz[]) WITH ORDINALITY AS s(sent_at, ord)
        USING (ord)
)
UPDATE outbox_events e
SET status          = 'sent',
    sent_at         = inp.sent_at,
    last_attempt_at = inp.sent_at,
    reserved_by     = NULL
    FROM inp
WHERE e.event_id = inp.event_id
    AND e.status = 'processing'
    AND e.reserved_by = sqlc.arg(process_id);

-- name: MarkOutboxEventsAsPending :exec
WITH inp AS (
    SELECT i.event_id, a.last_attempt_at, n.next_attempt_at, r.last_error
    FROM unnest(sqlc.arg(event_ids)::uuid[]) WITH ORDINALITY AS i(event_id, ord)
    JOIN unnest(sqlc.arg(last_attempt_ats)::timestamptz[]) WITH ORDINALITY AS a(last_attempt_at, ord)
        USING (ord)
    JOIN unnest(sqlc.arg(next_attempt_ats)::timestamptz[]) WITH ORDINALITY AS n(next_attempt_at, ord)
        USING (ord)
    JOIN unnest(sqlc.arg(last_errors)::text[]) WITH ORDINALITY AS r(last_error, ord)
        USING (ord)
)
UPDATE outbox_events e
SET status          = 'pending',
    attempts        = e.attempts + 1,
    last_attempt_at = inp.last_attempt_at,
    next_attempt_at = inp.next_attempt_at,
    last_error      = inp.last_error,
    reserved_by     = NULL
FROM inp
WHERE e.event_id = inp.event_id
    AND e.status = 'processing'
    AND e.reserved_by = sqlc.arg(process_id);


-- name: MarkOutboxEventsAsFailed :exec
WITH inp AS (
    SELECT i.event_id, r.last_error
    FROM unnest(sqlc.arg(event_ids)::uuid[]) WITH ORDINALITY AS i(event_id, ord)
    JOIN unnest(sqlc.arg(last_errors)::text[]) WITH ORDINALITY AS r(last_error, ord)
        USING (ord)
)
UPDATE outbox_events e
SET status          = 'failed',
    attempts        = e.attempts + 1,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    last_error      = inp.last_error,
    reserved_by     = NULL
    FROM inp
WHERE e.event_id = inp.event_id
    AND e.status = 'processing'
    AND e.reserved_by = sqlc.arg(process_id);

-- name: CleanProcessingOutboxEvents :exec
UPDATE outbox_events
SET status = 'pending',
    reserved_by = NULL,
    last_attempt_at = NULL
WHERE status = 'processing';

-- name: CleanReservedProcessingOutboxEvents :exec
UPDATE outbox_events
SET reserved_by = NULL
WHERE status = 'processing'
    AND reserved_by = sqlc.arg(process_id);

-- name: CleanFailedOutboxEvents :exec
UPDATE outbox_events
SET status = 'pending',
    attempts = 0,
    last_attempt_at = NULL,
    next_attempt_at = (now() AT TIME ZONE 'UTC')
WHERE status = 'failed';