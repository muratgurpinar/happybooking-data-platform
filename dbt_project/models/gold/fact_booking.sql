{{ config(materialized='view') }}

SELECT
    booking_id,
    hotel_id,
    customer_id,
    booking_date,
    checkin_date,
    checkout_date,
    nights,
    adults,
    children,
    infants,
    room_type,
    rooms_booked,
    booking_channel,
    booking_source,
    booking_status,
    is_cancelled,
    cancellation_date,
    cancellation_reason,
    total_price,
    room_price,
    tax_amount,
    service_fee,
    paid_amount,
    payment_status,
    payment_method,
    promotion_code,
    discount_amount,
    special_requests
FROM Silver.hotel_silver
WHERE booking_id IS NOT NULL
