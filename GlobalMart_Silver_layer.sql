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


------------- tables for Silver layer 

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



----------------------------------------------------- Merge and Task for ingestion data in silver stage 

----- For IOT data 

CREATE OR REPLACE TASK GLOBAL_MART_DB.STAGING.process_json
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
ALTER TASK global_mart_db.Staging.process_json resume;
select * from global_mart_db.raw.iot_events_raw;
select * from global_mart_db.raw.stream_json_raw;
select * from global_mart_db.staging.stg_json_sensor;
describe TASK global_mart_db.staging.process_json;


--------- For Csv files 

CREATE OR REPLACE TASK global_mart_db.Staging.process_csv
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
ALTER TASK global_mart_db.Staging.process_csv resume;
select * from global_mart_db.raw.pos_raw;
select * from global_mart_db.raw.stream_csv_raw;
select * from global_mart_db.staging.stg_csv_transaction;
describe TASK global_mart_db.Staging.process_csv;

--------------- For Parquet Data 

CREATE OR REPLACE TASK global_mart_db.staging.process_parquet
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
ALTER TASK global_mart_db.Staging.process_parquet resume; 
select * from global_mart_db.raw.erp_parquet_raw;
select * from GLOBAL_MART_DB.RAW.STREAM_PARQ_RAW;
select * from GLOBAL_MART_DB.STAGING.STG_ERP_PARQUET;
describe TASK global_mart_db.Staging.process_parquet; 