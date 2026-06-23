-- models/staging/stg_fraud_alerts.sql
-- Staging: clean và cast fraud alerts từ PostgreSQL public.fraud_alerts

SELECT
    id::integer                     AS alert_id,
    caller_id,
    window_start::timestamp         AS window_start,
    window_end::timestamp           AS window_end,
    call_count::integer             AS call_count,
    is_fraud::boolean               AS is_fraud,
    detected_at::timestamp          AS detected_at,
    -- Tính duration window để audit
    EXTRACT(EPOCH FROM (window_end - window_start)) / 60 AS window_duration_minutes
FROM {{ source('public', 'fraud_alerts') }}
WHERE is_fraud = TRUE
  AND caller_id IS NOT NULL
