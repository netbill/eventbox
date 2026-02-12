-- name: InsertInboxEvent :one
INSERT INTO inbox_events (
    event_id,
    topic, key, type, version, producer, payload, partition, kafka_offset,
    status, attempts, next_attempt_at,
    produced_at
) VALUES (
    sqlc.arg(event_id),
    sqlc.arg(topic),
    sqlc.arg(key),
    sqlc.arg(type),
    sqlc.arg(version),
    sqlc.arg(producer),
    sqlc.arg(payload)::jsonb,
    sqlc.arg(partition),
    sqlc.arg(kafka_offset),
    sqlc.arg(status),
    sqlc.arg(attempts),
    sqlc.arg(next_attempt_at),
    sqlc.arg(produced_at)
)
ON CONFLICT (event_id) DO NOTHING
RETURNING *;

-- name: GetInboxEventByID :one
SELECT *
FROM inbox_events
WHERE event_id = sqlc.arg(event_id);

-- name: ReserveInboxEvents :many
WITH ready AS (
    SELECT *
    FROM inbox_events
    WHERE status = 'pending'
        AND reserved_by IS NULL
        AND next_attempt_at <= (now() AT TIME ZONE 'UTC')
    ORDER BY produced_at, partition, kafka_offset
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(sort_limit)
),
key_heads AS (
    -- головы только внутри ограниченного ready-набора
    SELECT DISTINCT ON (r.topic, r.key)
        r.topic,
        r.key,
        r.produced_at AS head_produced_at
    FROM ready r
    -- не берём key, который уже processing (проверка по основной таблице быстрая через индекс processing_key_idx)
    WHERE NOT EXISTS (
        SELECT 1
        FROM inbox_events p
        WHERE p.topic = r.topic
            AND p.key = r.key
            AND p.status = 'processing'
            AND p.reserved_by IS NOT NULL
    )
    ORDER BY r.topic, r.key, r.produced_at
),
picked AS (
    -- теперь выбираем события ТОЛЬКО по выбранным key
    SELECT e.event_id
    FROM inbox_events e
    JOIN key_heads k ON k.topic = e.topic AND k.key = e.key
    WHERE e.status = 'pending'
        AND e.reserved_by IS NULL
        AND e.next_attempt_at <= (now() AT TIME ZONE 'UTC')
    ORDER BY k.head_produced_at, e.produced_at, e.kafka_offset
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_limit)
)
UPDATE inbox_events e
SET reserved_by = sqlc.arg(worker_id),
    status      = 'processing'
WHERE e.event_id IN (SELECT event_id FROM picked)
RETURNING *;

-- name: MarkInboxEventAsProcessed :one
UPDATE inbox_events
SET
    status = 'processed',
    attempts = attempts + 1,
    reserved_by = NULL,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    processed_at = (now() AT TIME ZONE 'UTC'),
    last_error = NULL
WHERE event_id = ANY(sqlc.arg(event_id)::uuid)
    AND status = 'processing'
    AND reserved_by = sqlc.arg(worker_id)
RETURNING *;

-- name: MarkInboxEventAsPending :one
UPDATE inbox_events
SET
    status = 'pending',
    attempts = attempts + 1,
    reserved_by = NULL,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    next_attempt_at = (sqlc.arg(next_attempt_at)::timestamptz),
    last_error = sqlc.arg(last_error)
WHERE event_id = (sqlc.arg(event_id)::uuid)
    AND status = 'processing'
    AND reserved_by = sqlc.arg(worker_id)
RETURNING *;

-- name: MarkInboxEventAsFailed :one
UPDATE inbox_events
SET
    status = 'failed',
    attempts = attempts + 1,
    reserved_by = NULL,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    last_error = sqlc.arg(last_error)
WHERE event_id = ANY(sqlc.arg(event_id)::uuid)
    AND status = 'processing'
    AND reserved_by = sqlc.arg(worker_id)
RETURNING *;

-- name: CleanProcessingInboxEvents :exec
UPDATE inbox_events
SET
    status = 'pending',
    reserved_by = NULL,
    next_attempt_at = (now() AT TIME ZONE 'UTC')
WHERE status = 'processing';

-- name: CleanReservedProcessingInboxEvents :exec
UPDATE inbox_events
SET
    status = 'pending',
    reserved_by = NULL,
    next_attempt_at = (now() AT TIME ZONE 'UTC')
WHERE status = 'processing'
    AND reserved_by = ANY(sqlc.arg(worker_ids)::text[]);

-- name: CleanFailedInboxEvents :exec
UPDATE inbox_events
SET
    status = 'pending',
    attempts = 0,
    next_attempt_at = (now() AT TIME ZONE 'UTC')
WHERE status = 'failed';
