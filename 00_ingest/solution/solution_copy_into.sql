-- Solution for how to ingest data into Snowflake using COPY INTO command

USE ROLE accountadmin;

-- create tasty_bytes database
CREATE OR REPLACE DATABASE tasty_bytes;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA tasty_bytes.raw_pos;

/*--
file format and stage creation
--*/

CREATE OR REPLACE FILE FORMAT tasty_bytes.public.csv_ff
type = 'csv';

CREATE OR REPLACE STAGE tasty_bytes.public.s3load
url = 's3://sfquickstarts/tasty-bytes-builder-education/'
file_format = tasty_bytes.public.csv_ff;

-- country table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.country
(
   country_id NUMBER(18,0),
   country VARCHAR(16777216),
   iso_currency VARCHAR(3),
   iso_country VARCHAR(2),
   city_id NUMBER(19,0),
   city VARCHAR(16777216),
   city_population VARCHAR(16777216)
);


-- country table load
COPY INTO tasty_bytes.raw_pos.country
FROM @tasty_bytes.public.s3load/raw_pos/country/;