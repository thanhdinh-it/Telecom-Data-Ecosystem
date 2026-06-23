-- models/marts/fact_monthly_usage.sql
-- Fact table: monthly usage per subscriber
-- Join dim_customer để enrich với customer attributes

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['u.caller_id', 'u.month']) }}  AS usage_sk,

    -- Dimensions
    u.caller_id,
    u.month,
    c.plan_type,
    c.customer_tenure_segment,
    c.is_active,

    -- Volume metrics
    u.monthly_total_calls,
    u.monthly_total_duration_sec,
    u.monthly_total_duration_hours,
    u.monthly_voice_calls,
    u.monthly_video_calls,
    u.voice_call_pct,

    -- Engagement
    u.active_days_in_month,
    u.avg_call_duration_sec,
    u.avg_daily_calls,

    -- Network
    u.max_towers_in_day,
    u.avg_towers_per_day,
    u.avg_unique_callee_per_day,

    CURRENT_TIMESTAMP   AS dbt_updated_at

FROM {{ ref('int_customer_monthly_usage') }} u
LEFT JOIN {{ ref('dim_customer') }} c
    ON u.caller_id = c.customer_id
