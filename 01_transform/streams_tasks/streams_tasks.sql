USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

/* 
Streams and tasks for incremental processing of data
*/


-- Create a physical table that contains the same
-- Hamburg weather and sales data as the view
CREATE OR REPLACE TABLE tasty_bytes.harmonized.weather_hamburg_st
(
    date DATE,
    city_name VARCHAR(16777216),
    country_desc VARCHAR(16777216),
    daily_sales NUMBER(38,2),
    avg_temperature_fahrenheit NUMBER(38,2),
    avg_temperature_celsius NUMBER(38,2),
    avg_precipitation_inches NUMBER(38,2),
    avg_precipitation_millimeters NUMBER(38,2),
    max_wind_speed_100m_mph NUMBER(38,2),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load February 2022 Hamburg data (same as what the view would show)
INSERT INTO tasty_bytes.harmonized.weather_hamburg_st
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph,
    CURRENT_TIMESTAMP() AS last_updated
FROM harmonized.daily_weather_v fd
LEFT JOIN harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;

-- Verify initial data load
SELECT * FROM tasty_bytes.harmonized.weather_hamburg_st ORDER BY date ASC LIMIT 5;


-- Create streams to track changes
-- Streams capture changes (INSERT/UPDATE/DELETE) on source tables

-- Stream on order_header to track new orders
CREATE OR REPLACE STREAM tasty_bytes.raw_pos.order_header_stream
ON TABLE tasty_bytes.raw_pos.order_header;

-- Stream on order_detail to track new order line items
CREATE OR REPLACE STREAM tasty_bytes.raw_pos.order_detail_stream
ON TABLE tasty_bytes.raw_pos.order_detail;

-- Check streams (ought to be empty initially):
SELECT 'order_header_stream' AS stream_name, COUNT(*) AS records
FROM tasty_bytes.raw_pos.order_header_stream

UNION ALL

SELECT 'order_detail_stream' AS stream_name, COUNT(*) AS records
FROM tasty_bytes.raw_pos.order_detail_stream;


-- Create task to process stream data incrementally
-- Tasks run SQL statements on a schedule or when triggered
CREATE OR REPLACE TASK tasty_bytes.harmonized.update_hamburg_weather_st_task
    WAREHOUSE = compute_wh
    SCHEDULE = '5 MINUTES'  -- Run every 5 minutes
WHEN
    SYSTEM$STREAM_HAS_DATA('tasty_bytes.raw_pos.order_header_stream')  -- Only run if stream has data
AS
    MERGE INTO tasty_bytes.harmonized.weather_hamburg_st AS target
    USING (
        -- Query streams for new orders and join with weather data
        SELECT
            fd.date_valid_std AS date,
            fd.city_name,
            fd.country_desc,
            ZEROIFNULL(SUM(od.price)) AS daily_sales,
            ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
            ROUND(AVG(analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
            ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
            ROUND(AVG(analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
            MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
        FROM tasty_bytes.raw_pos.order_header_stream oh
        JOIN tasty_bytes.raw_pos.order_detail_stream od
            ON oh.order_id = od.order_id
        JOIN tasty_bytes.raw_pos.truck t
            ON oh.truck_id = t.truck_id
        JOIN harmonized.daily_weather_v fd
            ON fd.date_valid_std = DATE(oh.order_ts)
            AND fd.city_name = t.primary_city
            AND fd.country_desc = t.country
        WHERE oh.METADATA$ACTION = 'INSERT'  -- Only process new inserts
            AND fd.country_desc = 'Germany'
            AND fd.city = 'Hamburg'
        GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
    ) AS source
    ON target.date = source.date
        AND target.city_name = source.city_name
        AND target.country_desc = source.country_desc
    WHEN MATCHED THEN
        UPDATE SET
            target.daily_sales = target.daily_sales + source.daily_sales,
            target.avg_temperature_fahrenheit = source.avg_temperature_fahrenheit,
            target.avg_temperature_celsius = source.avg_temperature_celsius,
            target.avg_precipitation_inches = source.avg_precipitation_inches,
            target.avg_precipitation_millimeters = source.avg_precipitation_millimeters,
            target.max_wind_speed_100m_mph = source.max_wind_speed_100m_mph,
            target.last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (date, city_name, country_desc, daily_sales, avg_temperature_fahrenheit,
                avg_temperature_celsius, avg_precipitation_inches, avg_precipitation_millimeters,
                max_wind_speed_100m_mph, last_updated)
        VALUES (source.date, source.city_name, source.country_desc, source.daily_sales,
                source.avg_temperature_fahrenheit, source.avg_temperature_celsius,
                source.avg_precipitation_inches, source.avg_precipitation_millimeters,
                source.max_wind_speed_100m_mph, CURRENT_TIMESTAMP());

-- Tasks are created in SUSPENDED state by default. Resume task to activate it:
ALTER TASK tasty_bytes.harmonized.update_hamburg_weather_task RESUME;

-- Check stream status (reusable query)
SELECT 'order_header_stream' AS stream_name, COUNT(*) AS records
FROM tasty_bytes.raw_pos.order_header_stream
UNION ALL
SELECT 'order_detail_stream' AS stream_name, COUNT(*) AS records
FROM tasty_bytes.raw_pos.order_detail_stream;

-- Simulate new orders arriving by calling the stored procedure
CALL tasty_bytes.public.simulate_new_orders(100);

-- Re-run the stream status query above to see new records in the streams

-- Execute the task to process stream data
EXECUTE TASK tasty_bytes.harmonized.update_hamburg_weather_task;

-- Re-run the stream status query, should be empty now

-- View updated table (see last_updated column)
SELECT * FROM tasty_bytes.harmonized.weather_hamburg_st
ORDER BY last_updated DESC
LIMIT 10;