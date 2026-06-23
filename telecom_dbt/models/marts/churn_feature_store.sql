-- models/marts/churn_feature_store.sql
-- Data Mart cuối: feature store cho churn prediction ML model
-- Tổng hợp tất cả features theo từng customer (last 3 months)

WITH monthly_usage AS (
    SELECT * FROM {{ ref('int_customer_monthly_usage') }}
),

-- Chỉ lấy 3 tháng gần nhất
recent_usage AS (
    SELECT *
    FROM monthly_usage
    WHERE month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
),

-- Fraud history
fraud_history AS (
    SELECT
        caller_id,
        COUNT(*)                        AS total_fraud_incidents,
        MAX(detected_at)                AS last_fraud_detected_at,
        SUM(call_count)                 AS total_fraud_calls
    FROM {{ ref('stg_fraud_alerts') }}
    GROUP BY 1
),

-- Customer base
customer_base AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

-- Aggregated 3-month features per customer
usage_features AS (
    SELECT
        caller_id,
        COUNT(DISTINCT month)                               AS months_active_in_3m,
        AVG(monthly_total_calls)                            AS avg_monthly_calls_3m,
        AVG(monthly_total_duration_sec)                     AS avg_monthly_duration_sec_3m,
        AVG(active_days_in_month)                           AS avg_active_days_3m,
        AVG(avg_call_duration_sec)                          AS avg_call_duration_3m,
        AVG(voice_call_pct)                                 AS avg_voice_call_pct_3m,
        AVG(avg_unique_callee_per_day)                      AS avg_unique_callee_3m,

        -- Trend: so sánh tháng gần nhất vs tháng cũ nhất trong 3 tháng
        MAX(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                 THEN monthly_total_calls END)
        - MAX(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
                   THEN monthly_total_calls END)            AS call_trend_1m_vs_3m

    FROM recent_usage
    GROUP BY 1
)

SELECT
    c.customer_id,
    c.full_name,
    c.plan_type,
    c.registration_date,
    c.days_since_registration,
    c.customer_tenure_segment,
    c.is_active,

    -- Usage features (last 3 months)
    COALESCE(u.months_active_in_3m, 0)             AS months_active_in_3m,
    COALESCE(u.avg_monthly_calls_3m, 0)             AS avg_monthly_calls_3m,
    COALESCE(u.avg_monthly_duration_sec_3m, 0)      AS avg_monthly_duration_sec_3m,
    COALESCE(u.avg_active_days_3m, 0)               AS avg_active_days_3m,
    COALESCE(u.avg_call_duration_3m, 0)             AS avg_call_duration_3m,
    COALESCE(u.avg_voice_call_pct_3m, 0)            AS avg_voice_call_pct_3m,
    COALESCE(u.avg_unique_callee_3m, 0)             AS avg_unique_callee_3m,

    -- Trend feature: âm = usage giảm (churn signal quan trọng)
    COALESCE(u.call_trend_1m_vs_3m, 0)             AS call_trend_1m_vs_3m,

    -- Churn signal derived
    CASE
        WHEN u.avg_monthly_calls_3m IS NULL                  THEN 1  -- không có data = at risk
        WHEN u.call_trend_1m_vs_3m < -20                     THEN 1  -- giảm > 20 calls/tháng
        WHEN u.avg_active_days_3m < 5                        THEN 1  -- < 5 ngày/tháng
        ELSE 0
    END                                                       AS is_churn_risk,

    -- Fraud features
    COALESCE(f.total_fraud_incidents, 0)            AS total_fraud_incidents,
    COALESCE(f.total_fraud_calls, 0)                AS total_fraud_calls,
    CASE
        WHEN f.last_fraud_detected_at > CURRENT_DATE - INTERVAL '30 days' THEN 1
        ELSE 0
    END                                                       AS had_fraud_last_30d,

    CURRENT_TIMESTAMP                               AS feature_generated_at

FROM customer_base c
LEFT JOIN usage_features u
    ON c.customer_id = u.caller_id
LEFT JOIN fraud_history f
    ON c.customer_id = f.caller_id

-- Chỉ giữ active customers (inactive không cần predict churn)
WHERE c.is_active = TRUE
