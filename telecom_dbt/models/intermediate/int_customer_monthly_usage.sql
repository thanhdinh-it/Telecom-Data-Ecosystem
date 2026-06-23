-- models/intermediate/int_customer_monthly_usage.sql
-- Tổng hợp usage theo tháng per subscriber
-- Input: stg_cdr_daily → Output: một dòng per (caller_id, month)

WITH daily_cdr AS (
    SELECT * FROM {{ ref('stg_cdr_daily') }}
),

monthly_agg AS (
    SELECT
        caller_id,
        DATE_TRUNC('month', call_date)::date        AS month,

        -- Volume metrics
        SUM(total_calls)                            AS monthly_total_calls,
        SUM(total_duration_seconds)                 AS monthly_total_duration_sec,
        SUM(voice_calls)                            AS monthly_voice_calls,
        SUM(video_calls)                            AS monthly_video_calls,

        -- Engagement metrics
        COUNT(DISTINCT call_date)                   AS active_days_in_month,
        AVG(avg_duration_seconds)                   AS avg_call_duration_sec,
        AVG(total_calls)                            AS avg_daily_calls,

        -- Network usage
        MAX(towers_used)                            AS max_towers_in_day,
        AVG(towers_used)                            AS avg_towers_per_day,

        -- Callee diversity
        AVG(unique_callee_count)                    AS avg_unique_callee_per_day

    FROM daily_cdr
    GROUP BY 1, 2
)

SELECT
    caller_id,
    month,
    monthly_total_calls,
    monthly_total_duration_sec,
    ROUND(monthly_total_duration_sec / 3600.0, 2)   AS monthly_total_duration_hours,
    monthly_voice_calls,
    monthly_video_calls,
    ROUND(100.0 * monthly_voice_calls / NULLIF(monthly_total_calls, 0), 1) AS voice_call_pct,
    active_days_in_month,
    ROUND(avg_call_duration_sec, 2)                 AS avg_call_duration_sec,
    ROUND(avg_daily_calls, 2)                       AS avg_daily_calls,
    max_towers_in_day,
    ROUND(avg_towers_per_day, 2)                    AS avg_towers_per_day,
    ROUND(avg_unique_callee_per_day, 2)             AS avg_unique_callee_per_day

FROM monthly_agg
