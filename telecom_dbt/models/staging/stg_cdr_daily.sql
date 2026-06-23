-- models/staging/stg_cdr_daily.sql
-- Staging layer: rename + cast từ PySpark batch output
-- 1:1 với source table raw.daily_cdr_agg

SELECT
    caller_id,
    call_date::date                         AS call_date,
    total_calls::integer                    AS total_calls,
    total_duration_seconds::bigint          AS total_duration_seconds,
    ROUND(avg_duration_seconds::numeric, 2) AS avg_duration_seconds,
    unique_callee_count::integer            AS unique_callee_count,
    voice_calls::integer                    AS voice_calls,
    video_calls::integer                    AS video_calls,
    towers_used::integer                    AS towers_used,
    ROUND(avg_duration_minutes::numeric, 2) AS avg_duration_minutes
FROM {{ source('raw', 'daily_cdr_agg') }}
WHERE caller_id IS NOT NULL
  AND call_date IS NOT NULL
  AND total_calls > 0
