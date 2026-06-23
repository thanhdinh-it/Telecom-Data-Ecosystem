-- models/marts/fact_fraud_alerts.sql
-- Fact table: fraud alerts enriched với customer info

SELECT
    fa.alert_id,
    fa.caller_id,
    c.plan_type,
    c.customer_tenure_segment,
    fa.window_start,
    fa.window_end,
    fa.window_duration_minutes,
    fa.call_count,
    fa.is_fraud,
    fa.detected_at,

    -- Derived: thời điểm phát hiện
    DATE_TRUNC('day', fa.detected_at)::date     AS alert_date,
    EXTRACT(HOUR FROM fa.detected_at)::integer  AS alert_hour,

    CURRENT_TIMESTAMP                           AS dbt_updated_at

FROM {{ ref('stg_fraud_alerts') }} fa
LEFT JOIN {{ ref('dim_customer') }} c
    ON fa.caller_id = c.customer_id
