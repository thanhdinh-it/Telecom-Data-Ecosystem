-- models/marts/dim_customer.sql
-- Dimension table cho customers
-- Type 1 SCD (overwrite) — không track lịch sử thay đổi

SELECT
    customer_id,
    full_name,
    phone_number,
    registration_date,
    plan_type,
    is_active,

    -- Derived attributes
    CURRENT_DATE - registration_date                            AS days_since_registration,
    CASE
        WHEN CURRENT_DATE - registration_date < 90  THEN 'new'
        WHEN CURRENT_DATE - registration_date < 365 THEN 'growing'
        ELSE 'mature'
    END                                                         AS customer_tenure_segment,

    source_loaded_at,
    CURRENT_TIMESTAMP                                           AS dbt_updated_at

FROM {{ ref('stg_customers') }}
