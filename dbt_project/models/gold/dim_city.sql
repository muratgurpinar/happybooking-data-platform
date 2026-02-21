{{ config(materialized='view') }}

SELECT DISTINCT
    city,
    country,
    COUNT(booking_id)                        AS total_bookings,
    AVG(CAST(total_price AS FLOAT))          AS avg_booking_price,
    AVG(CAST(star_rating AS FLOAT))          AS avg_star_rating
FROM Silver.hotel_silver
WHERE city IS NOT NULL
GROUP BY city, country
