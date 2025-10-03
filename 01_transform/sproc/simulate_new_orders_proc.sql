USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

/*
This stored procedure simulates new orders arriving in raw tables by copying existing orders
with new order_ids and current timestamps. This is to demonstrate incremental
processing with streams and tasks.

Usage:
    CALL tasty_bytes.public.simulate_new_orders(100);  -- Generate 100 new orders
    CALL tasty_bytes.public.simulate_new_orders(50);   -- Generate 50 new orders
*/

CREATE OR REPLACE PROCEDURE tasty_bytes.public.simulate_new_orders(num_orders INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Simulate new orders in order_header table
    -- We offset order_id by 10 million to avoid conflicts with existing data
    INSERT INTO tasty_bytes.raw_pos.order_header
    SELECT
        order_id + 10000000 AS order_id,  -- New unique order IDs
        truck_id,
        location_id,
        customer_id,
        discount_id,
        shift_id,
        shift_start_time,
        shift_end_time,
        order_channel,
        CURRENT_TIMESTAMP() AS order_ts,  -- Current timestamp to simulate new orders
        served_ts,
        order_currency,
        order_amount,
        order_tax_amount,
        order_discount_amount,
        order_total
    FROM tasty_bytes.raw_pos.order_header
    WHERE order_id < 10000000  -- Only copy from original data, not previously simulated data
    LIMIT :num_orders;

    -- Simulate corresponding order_detail records
    -- Must maintain referential integrity with order_header
    INSERT INTO tasty_bytes.raw_pos.order_detail
    SELECT
        order_detail_id + 10000000 AS order_detail_id,  -- New unique detail IDs
        order_id + 10000000 AS order_id,  -- Match the new order_id
        menu_item_id,
        discount_id,
        line_number,
        quantity,
        unit_price,
        price,
        order_item_discount_amount
    FROM tasty_bytes.raw_pos.order_detail
    WHERE order_id IN (
        SELECT order_id
        FROM tasty_bytes.raw_pos.order_header
        WHERE order_id < 10000000
        LIMIT :num_orders
    );

    RETURN 'Successfully simulated ' || :num_orders || ' new orders with current timestamp';
END;
$$;

-- Test the procedure (optional - comment out if not needed)
-- CALL tasty_bytes.public.simulate_new_orders(10);

-- Verify the procedure was created
SHOW PROCEDURES LIKE 'simulate_new_orders' IN tasty_bytes.public;
