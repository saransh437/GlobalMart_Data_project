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
comment = 'silver layer';

create schema if not exists global_mart_db.marts
comment = 'golden layer';

-- Storage Integration-------------

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
 
 create file format  global_mart_db.integrations.format_json 
 type='JSON' 
 STRIP_OUTER_ARRAY=TRUE  
 COMMENT='json format for iot event bacth files';

 CREATE OR REPLACE FILE FORMAT global_mart_db.integrations.format_parquet
TYPE = PARQUET;

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
    loyalty_points      int 
    load_timestamp TIMESTAMP
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


-- create a pipe for raw tables ----------------------------------------------------------------

CREATE OR REPLACE PIPE global_mart_db.integrations.csv_pipe_raw
AUTO_INGEST = TRUE
AS
COPY INTO global_mart_db.raw.pos_raw
FROM
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



CREATE OR REPLACE PIPE global_mart_db.integrations.json_pipe_raw
AUTO_INGEST = TRUE
AS
COPY INTO global_mart_db.raw.iot_events_raw
FROM
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
FILE_FORMAT = global_mart_db.integrations.format_Json;  
describe  PIPE global_mart_db.integrations.json_pipe_raw;
select * from global_mart_db.raw.iot_events_raw;


CREATE OR REPLACE PIPE global_mart_db.integrations.parq_raw_pipe
AUTO_INGEST = TRUE
AS
COPY INTO global_mart_db.raw.erp_parquet_raw
FROM
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
FILE_FORMAT = global_mart_db.integrations.format_Parquet; 
describe  PIPE global_mart_db.integrations.parq_raw_pipe;
select * from global_mart_db.raw.ERP_PARQUET_RAW;


------------------- stream on bronze layer tables
create or replace stream global_mart_db.raw.stream_csv_raw 
on table global_mart_db.raw.pos_raw 
APPEND_ONLY = TRUE;
describe stream global_mart_db.raw.stream_csv_raw;
select * from global_mart_db.raw.stream_csv_raw;

create or replace stream global_mart_db.raw.stream_json_raw 
on table  global_mart_db.raw.iot_events_raw
APPEND_ONLY = TRUE;
describe stream global_mart_db.raw.stream_json_raw;
select * from global_mart_db.raw.stream_json_raw;

create or replace stream global_mart_db.raw.stream_parq_raw
on table global_mart_db.raw.erp_parquet_raw
append_only = True;
describe stream global_mart_db.raw.stream_parq_raw;
select * from global_mart_db.raw.stream_parq_raw;
------------------ tables for silver layer ----------------------

CREATE OR REPLACE TABLE global_mart_db.staging.stg_json_sensor (
    event_id STRING,
    event_type STRING,
    store_id STRING,
    store_name STRING,
    event_ts TIMESTAMP,
    device_id STRING,
    firmware STRING,
    battery_pct int,
    store_floor int,
    sensor_name string,
    sensor_value float,
    sensor_unit string,
    source_file STRING,
    loaded_at TIMESTAMP,
    processed_ts timestamp
);
select count(store_id),count(distinct store_id) from global_mart_db.staging.stg_csv_transaction;
CREATE OR REPLACE TABLE global_mart_db.staging.stg_csv_transaction (
    transaction_id      STRING,
    store_id            STRING,
    store_name          STRING,
    store_city          STRING,
    store_region        STRING,
    cashier_id          STRING,
    customer_id         STRING,
    transaction_date    DATE,
    transaction_time    TIME,
    transaction_ts      timestamp,
    product_sku         STRING,
    product_name        STRING,
    category            STRING,
    subcategory         STRING,
    quantity            int,
    unit_price          float,
    discount_pct        int,
    total_amount        float,
    line_total          float,
    payment_method      STRING,
    loyalty_points      int ,
    load_timestamp TIMESTAMP,
    file_name STRING,
    processed_time timestamp
);

CREATE OR REPLACE TABLE global_mart_db.staging.stg_erp_parquet(
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
    source_file STRING,
    processed_time timestamp
) ;

----------------------------- merge and task  into silver stg 

---- for json file -------------
CREATE OR REPLACE TASK global_mart_db.raw.process_json
WAREHOUSE = compute_wh
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.raw.stream_json_raw')
AS
MERGE INTO global_mart_db.staging.stg_json_sensor AS stg
USING (
    SELECT
        event_id,
        event_type,
        store_id,
        store_name,
        event_ts,
        device_id,
        raw_payload:metadata.firmware::STRING  as firmware,
        raw_payload:metadata.battery_pct::INT  as battery_pct,
        raw_payload:metadata.store_floor::INT   as store_floor,
        f.value:sensor::STRING  as sensor_name,
        f.value:value::FLOAT  as sensor_value,
        f.value:unit::STRING   as sensor_unit,
        source_file,
        loaded_at,
        CURRENT_TIMESTAMP()  as processed_ts
    FROM global_mart_db.raw.stream_json_raw,
         LATERAL FLATTEN(input => raw_payload:readings) f
) AS src
ON stg.event_id = src.event_id
AND stg.sensor_name = src.sensor_name
WHEN NOT MATCHED THEN
INSERT (
    event_id, event_type, store_id, store_name, event_ts, device_id, firmware, battery_pct,
    store_floor, sensor_name, sensor_value, sensor_unit,  source_file, loaded_at, processed_ts
)
VALUES (
    src.event_id,  src.event_type, src.store_id, src.store_name, src.event_ts, src.device_id, src.firmware,
    src.battery_pct, src.store_floor, src.sensor_name, src.sensor_value, src.sensor_unit, src.source_file,
    src.loaded_at, src.processed_ts
);
ALTER TASK global_mart_db.raw.process_json resume;
select * from global_mart_db.raw.iot_events_raw;
select * from global_mart_db.raw.stream_json_raw;
select * from global_mart_db.staging.stg_json_sensor;
describe TASK global_mart_db.raw.process_json;


----- for csv file -------------
CREATE OR REPLACE TASK global_mart_db.raw.process_csv
WAREHOUSE = compute_wh
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.raw.stream_csv_raw')
AS
MERGE INTO global_mart_db.staging.stg_csv_transaction AS stg
USING(
 SELECT 
    transaction_id,
    store_id,
    store_name,
    store_city,
    store_region,
    cashier_id,
    customer_id,
    transaction_date,
    transaction_time,
    CONCAT(transaction_date, ' ', transaction_time) AS transaction_ts,
    product_sku,
    product_name,
    category,
    subcategory,
    CASE WHEN quantity < 0 THEN 0  ELSE quantity END AS quantity,
    CASE WHEN unit_price < 0 THEN 0 ELSE unit_price END AS unit_price,
    CASE WHEN discount_pct < 0 THEN 0 ELSE discount_pct END AS discount_pct,
    total_amount,
    ( (CASE WHEN quantity < 0 THEN 0 ELSE quantity END) * 
       (CASE WHEN unit_price < 0 THEN 0 ELSE unit_price END) *
        (
            1 - (CASE WHEN discount_pct < 0 THEN 0 ELSE discount_pct END) / 100
        )
    ) AS line_total,
    CASE  WHEN LOWER(payment_method) = 'credit card' THEN 'CC' WHEN LOWER(payment_method) = 'debit card' THEN 'DC' ELSE payment_method
    END AS payment_method,
    loyalty_points,
    load_timestamp,
    file_name,
    CURRENT_TIMESTAMP() AS processed_time
FROM global_mart_db.raw.stream_csv_raw ) src 
ON stg.transaction_id = src.transaction_id
WHEN NOT MATCHED THEN
INSERT (
     transaction_id , store_id , store_name , store_city , store_region, cashier_id ,
    customer_id , transaction_date, transaction_time , transaction_ts , product_sku,
    product_name , category, subcategory , quantity , unit_price , discount_pct , total_amount,
    line_total , payment_method , loyalty_points, load_timestamp , file_name , processed_time 
)
VALUES (
    src.transaction_id , src.store_id , src.store_name , src.store_city , src.store_region, src.cashier_id ,
    src.customer_id , src.transaction_date, src.transaction_time , src.transaction_ts , src.product_sku,
    src.product_name , src.category, src.subcategory , src.quantity , src.unit_price , src.discount_pct , src.total_amount,
    src.line_total , src.payment_method , src.loyalty_points, src.load_timestamp , src.file_name , src.processed_time 
);
ALTER TASK global_mart_db.raw.process_csv resume;
select * from global_mart_db.raw.pos_raw;
select * from global_mart_db.raw.stream_csv_raw;
select * from global_mart_db.staging.stg_csv_transaction ;
describe TASK global_mart_db.raw.process_csv;

-------------- for parquet file 

CREATE OR REPLACE TASK global_mart_db.raw.process_parquet
WAREHOUSE = compute_wh
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.raw.stream_parq_raw')
AS
MERGE INTO global_mart_db.staging.stg_erp_parquet AS stg
using (
  SELECT
        order_id,
        order_date,
        store_id,
        store_city,
        supplier_id,
        supplier_name,
        supplier_city,
        product_sku,
        category,
        quantity_ordered,
        quantity_received,
        unit_cost,
        total_cost,
        order_status,
        expected_delivery,
        actual_delivery,
        warehouse_id,
        lead_time_days,
        is_late,
        load_time,
        source_file,
        CURRENT_TIMESTAMP() AS processed_time
        FROM global_mart_db.raw.stream_parq_raw ) src 
        ON stg.order_id = src.order_id
        WHEN NOT MATCHED THEN
        INSERT ( order_id,
        order_date, store_id, store_city, supplier_id, supplier_name, supplier_city,
        product_sku, category, quantity_ordered, quantity_received, unit_cost, total_cost, order_status, expected_delivery,
        actual_delivery, warehouse_id, lead_time_days, is_late, load_time, source_file, processed_time )
        values (
        src.order_id,
        src.order_date, src.store_id, src.store_city, src.supplier_id, src.supplier_name, src.supplier_city,
        src.product_sku, src.category, src.quantity_ordered, src.quantity_received, src.unit_cost, src.total_cost,   src.order_status,src.expected_delivery,
        src.actual_delivery, src.warehouse_id, src.lead_time_days, src.is_late, src.load_time, src.source_file, src.processed_time );
ALTER TASK global_mart_db.raw.process_parquet resume; 
select * from global_mart_db.raw.erp_parquet_raw;
select * from GLOBAL_MART_DB.RAW.STREAM_PARQ_RAW;
select * from GLOBAL_MART_DB.STAGING.STG_ERP_PARQUET ;
describe TASK global_mart_db.raw.process_parquet; 


--------------------------------------------------------golden layer  tables ----------------------------------------------------------

--## Daily Sales Fact 
create or replace table marts.daily_sales_fact(
 report_date timestamp,
 Store_region string,
 store_city string,
 store_name string,
 store_id string,
 category string,
 total_revenue_generated int,
 total_units_sold int,
 total_transaction_done int,
 average_cart_size int,
 total_unique_customer int,
 date_update date) ;

insert into global_mart_db.marts.daily_sales_fact
  select 
  transaction_date as report_date,
  Store_region,
  store_city ,
  store_name,
  store_id,
  category ,
  sum(Total_amount) as  total_revenue_generated, 
  sum(quantity) as total_units_sold ,
  count(transaction_id) as total_transaction_done,
  avg(Total_amount) as average_cart_size,
  count(distinct customer_id) as total_unique_customer,
  current_date as date_update
  from  global_mart_db.staging.stg_csv_transaction
  group by transaction_date ,Store_region,store_city ,store_name,store_id , category order by transaction_date;

 select * from  global_mart_db.marts.daily_sales_fact;



----#### gross margin fact

with csv_cte as (
select store_id,store_name, category,sum(line_total) as line_total ,sum(quantity) as number_of_unites_sold,count(distinct customer_id) as unique_customer_id   from global_mart_db.staging.stg_csv_transaction group by store_id ,store_name,category)
,
 parque_cte as (
select store_id , category , avg(unit_cost) as per_item_cost from  global_mart_db.staging.stg_erp_parquet group by store_id,category 
order by store_id,category)

select
  p.store_id,
  c.store_name,
  p.category,
  c.line_total as total_revenue_generated,        
  (c.number_of_unites_sold *p.per_item_cost) as  total_cost_generated,      
  (c.line_total -total_cost_generated) as gross_profit_margin ,        
  (gross_profit_margin/total_revenue_generated)*100 as gross_margin_percentage,    
  c.number_of_unites_sold,   
   c.unique_customer_id 
  from csv_cte as c   
  join parque_cte as p 
  on  LOWER(p.store_id) = LOWER(c.store_id) 
  and LOWER(p.category) = LOWER(c.category)  
  order by  c.store_id,p.category;
