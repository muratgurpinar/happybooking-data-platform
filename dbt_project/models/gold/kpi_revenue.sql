{{ config(materialized='view') }}


SELECT
    FORMAT(booking_date, 'yyyy-MM')          AS booking_month,
    hotel_type,
    room_type,
    booking_channel,
    COUNT(booking_id)                        AS total_bookings,
    SUM(CAST(total_price AS FLOAT))          AS total_revenue,
    AVG(CAST(total_price AS FLOAT))          AS avg_revenue,
    SUM(CASE WHEN LOWER(CAST(is_cancelled AS VARCHAR)) = 'true'
        THEN 1 ELSE 0 END)                   AS cancelled_bookings,
    SUM(CAST(discount_amount AS FLOAT))      AS total_discount
FROM Silver.hotel_silver
WHERE booking_date IS NOT NULL
GROUP BY FORMAT(booking_date, 'yyyy-MM'), hotel_type, room_type, booking_channel
