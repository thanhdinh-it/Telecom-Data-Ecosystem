-- models/staging/stg_customers.sql
-- Staging layer: chỉ rename cột và cast kiểu dữ liệu
-- Không có business logic ở đây

SELECT
    customer_id,
    full_name,
    phone_number,
    registration_date::date         AS registration_date,
    LOWER(TRIM(plan_type))          AS plan_type,
    is_active::boolean              AS is_active,
    loaded_at                       AS source_loaded_at
FROM {{ source('raw', 'customers') }}
-- Loại bỏ dòng không có customer_id (không thể track)
WHERE customer_id IS NOT NULL
