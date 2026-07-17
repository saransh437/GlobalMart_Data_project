-- global_mart_db => database
-- integration ==> storage integration schema , external stage ,fileformate
-- s3_stage - external stage name
-- raw schema => raw tables - iot_events_raw
-- s3 => iot => iot json file 

create database if not exists global_mart_db
comment = 'GlobalMart retail data platfrom' ;

-- schemas

create schema if not exists global_mart_db.integrations
comment = 'Storage integration ,, file formats,external stages';


create schema if not exists global_mart_db.RAW
comment = 'for raw tables';


create schema if not exists global_mart_db.Staging
comment = 'Silver layer tables';

create schema if not exists global_mart_db.gold
comment = 'gold layer tables';


----- storage integration 
CREATE or replace STORAGE INTEGRATION s3_integration_v1
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::101001870181:role/globalmart-data-v1'
    STORAGE_ALLOWED_LOCATIONS = ('s3://globalmart-data-project/');

desc storage integration s3_integration_v1;


-- stages ----------------------

create or replace stage  global_mart_db.integrations.s3_stage
url = 's3://globalmart-data-project/'
storage_integration = s3_integration_v1;

LIST @global_mart_db.integrations.s3_stage;



 -- ----------------- File Formats

 CREATE OR REPLACE FILE FORMAT global_mart_db.integrations.format_csv
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;
 
 create or replace file format  global_mart_db.integrations.format_json 
 type='JSON' 
 STRIP_OUTER_ARRAY=TRUE  
 COMMENT='json format for iot event bacth files';

create or replace file format global_mart_db.integrations.format_parquet
type = PARQUET;




-- Tables for Raw data ----------------------------
CREATE OR REPLACE TABLE global_mart_db.raw.iot_events_raw (
    event_id STRING,
    event_type STRING,
    store_id STRING,
    store_name STRING,
    event_ts TIMESTAMP,
    device_id STRING,
    raw_payload VARIANT,
    source_file STRING,
    loaded_at TIMESTAMP
);
 
CREATE OR REPLACE TABLE global_mart_db.raw.pos_raw (
    transaction_id      STRING,
    store_id            STRING,
    store_name          STRING,
    store_city          STRING,
    store_region        STRING,
    cashier_id          STRING,
    customer_id         STRING,
    transaction_date    DATE,
    transaction_time    TIME,
    product_sku         STRING,
    product_name        STRING,
    category            STRING,
    subcategory         STRING,
    quantity            int,
    unit_price          float,
    discount_pct        int,
    total_amount        float,
    payment_method      STRING,
    loyalty_points      int , 
    load_timestamp TIMESTAMP ,
    file_name STRING
);

CREATE OR REPLACE TABLE global_mart_db.raw.erp_parquet_raw(
    order_id      STRING,
    order_date    TIMESTAMP,
    store_id          STRING,
    store_city          STRING,
    supplier_id        STRING,
    supplier_name          STRING,
    supplier_city         STRING,
    product_sku         STRING,
    category            STRING,
    quantity_ordered    int,
    quantity_received   int,
    unit_cost          float,
    total_cost        float,
    order_status      STRING,
    expected_delivery date,
    actual_delivery   date,
    warehouse_id      string,
    lead_time_days    int,
    is_late           Boolean,
    load_time TIMESTAMP,
    source_file STRING
) ;


-- create a pipe for raw tables which automatically load the data from S3 when a new file will be uploaded ---------------------------------------------

create or replace pipe global_mart_db.integrations.csv_pipe_raw
AUTO_INGEST = TRUE
AS
copy into global_mart_db.raw.pos_raw
from
(
  SELECT
        $1,  
        $2,  
        $3,  
        $4,  
        $5,  
        $6,  
        $7,  
        $8,  
        $9,  
        $10, 
        $11, 
        $12,
        $13, 
        $14,
        $15, 
        $16, 
        $17, 
        $18, 
        $19, 
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @global_mart_db.integrations.S3_STAGE/Pos/
)
FILE_FORMAT = global_mart_db.integrations.format_csv;
describe  PIPE global_mart_db.integrations.csv_pipe_raw;
select * from global_mart_db.raw.pos_raw;


CREATE or replace pipe global_mart_db.integrations.json_raw_pipe
AUTO_INGEST = TRUE
as
Copy into GLOBAL_MART_DB.RAW.IOT_EVENTS_RAW
from 
(
 SELECT
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:store_id::STRING,
        $1:store_name::STRING,
        $1:timestamp::TIMESTAMP,
        $1:device_id::STRING,
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()
    FROM @global_mart_db.integrations.S3_STAGE/iot/
)
File_format = GLOBAL_MART_DB.INTEGRATIONS.FORMAT_JSON;
describe  PIPE global_mart_db.integrations.json_raw_pipe;
select * from global_mart_db.raw.iot_events_raw;


create or replace pipe global_mart_db.integrations.parquet_raw_pipe
auto_ingest = TRUE
as 
copy into GLOBAL_MART_DB.RAW.ERP_PARQUET_RAW
from
( 
        SELECT
        $1:order_id::STRING,
        $1:order_date::TIMESTAMP,
        $1:store_id::STRING,
        $1:store_city::STRING,
        $1:supplier_id::STRING,
        $1:supplier_name::STRING,
        $1:supplier_city::STRING,
        $1:product_sku::STRING,
        $1:category::STRING,
        $1:quantity_ordered::INT,
        $1:quantity_received::INT,
        $1:unit_cost::FLOAT,
        $1:total_cost::FLOAT,
        $1:order_status::STRING,
        $1:expected_delivery::DATE,
        $1:actual_delivery::DATE,
        $1:warehouse_id::STRING,
        $1:lead_time_days::INT,
        $1:is_late::BOOLEAN,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @global_mart_db.integrations.S3_STAGE/Parquet/
      )
file_format= GLOBAL_MART_DB.INTEGRATIONS.FORMAT_PARQUET;
DESCRIBE pipe GLOBAL_MART_DB.INTEGRATIONS.PARQUET_RAW_PIPE;
select * from GLOBAL_MART_DB.RAW.ERP_PARQUET_RAW;


    