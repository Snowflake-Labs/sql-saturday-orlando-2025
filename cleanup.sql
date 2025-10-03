USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

/*--
CLEANUP SCRIPT

Run this script to remove all objects created during the lab and stop any running tasks

CRITICAL: This stops background tasks that would otherwise continue consuming credits
--*/

/*--
SUSPEND AND DROP TASKS
Tasks run on schedules and will continue consuming warehouse credits until suspended
Always suspend before dropping to ensure clean shutdown
--*/

-- Suspend task first
ALTER TASK IF EXISTS tasty_bytes.harmonized.update_hamburg_weather_task SUSPEND;

-- Drop the task
DROP TASK IF EXISTS tasty_bytes.harmonized.update_hamburg_weather_task;

-- Verify no tasks are running
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'UPDATE_HAMBURG_WEATHER_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC
LIMIT 5;


/*--
DROP STREAMS
--*/

DROP STREAM IF EXISTS tasty_bytes.raw_pos.order_header_stream;
DROP STREAM IF EXISTS tasty_bytes.raw_pos.order_detail_stream;


/*--
DROP PHYSICAL TABLES
Remove tables created for streams + tasks approach
--*/

DROP TABLE IF EXISTS tasty_bytes.harmonized.weather_hamburg_st;


/*--
DROP STORED PROCEDURE
Remove the data simulation stored procedure
--*/

DROP PROCEDURE IF EXISTS tasty_bytes.public.simulate_new_orders(INT);


/*--
DELETE SIMULATED DATA
Remove simulated orders from the raw tables (order_id >= 10000000).
This restores the tables to their original state.
--*/

DELETE FROM tasty_bytes.raw_pos.order_header WHERE order_id >= 10000000;
DELETE FROM tasty_bytes.raw_pos.order_detail WHERE order_detail_id >= 10000000;

-- Verify deletion
SELECT
    'order_header' AS table_name,
    COUNT(*) AS simulated_records_remaining
FROM tasty_bytes.raw_pos.order_header
WHERE order_id >= 10000000

UNION ALL

SELECT
    'order_detail' AS table_name,
    COUNT(*) AS simulated_records_remaining
FROM tasty_bytes.raw_pos.order_detail
WHERE order_detail_id >= 10000000;

-- Should show 0 records for both tables


/*--
DROP VIEWS (OPTIONAL)
--*/

-- DROP VIEW IF EXISTS tasty_bytes.harmonized.weather_hamburg;
-- DROP VIEW IF EXISTS tasty_bytes.harmonized.hamburg_wind_v;
-- DROP VIEW IF EXISTS tasty_bytes.harmonized.daily_weather_v;


/*--
DROP UDFs (OPTIONAL)
--*/

-- DROP FUNCTION IF EXISTS tasty_bytes.analytics.fahrenheit_to_celsius(NUMBER);
-- DROP FUNCTION IF EXISTS tasty_bytes.analytics.inch_to_millimeter(NUMBER);


/*--
VERIFICATION
Check that no lab objects remain active
--*/

-- Check for any remaining tasks in the database
SHOW TASKS IN DATABASE tasty_bytes;

-- Check for any remaining streams
SHOW STREAMS IN DATABASE tasty_bytes;

SELECT 'Cleanup complete! All lab objects have been removed.' AS status;


/*--
OPTIONAL: COMPLETE TEARDOWN
If you want to remove the entire demo environment uncomment the following:
--*/

-- DROP DATABASE IF EXISTS tasty_bytes;
-- DROP WAREHOUSE IF EXISTS compute_wh;