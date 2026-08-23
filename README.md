GlobalMart Data Platform — Snowflake Medallion Pipeline

A Snowflake pipeline for GlobalMart that ingests POS, IoT, and ERP data and models it through Bronze → Silver → Gold layers, ending in a galaxy schema.

S3 (CSV/JSON/Parquet) → Snowpipe → BRONZE (raw)
     → Streams → Task+MERGE → SILVER (staging)
          → Dims/Facts/Aggs → GOLD (gold)
Schemas
integrations — storage integration, stage, file formats, pipes
raw — Bronze landing tables
staging — Silver, cleaned/deduped tables
gold — dimensions, facts, aggregates


Bronze (GlobalMart_bronze_layer.sql)
Storage integration s3_integration_v1 + stage s3_stage → s3://globalmart-data-project/
File formats: format_csv, format_json (strip outer array), format_parquet
Raw tables: pos_raw (CSV), iot_events_raw (JSON, header cols + raw_payload VARIANT), erp_parquet_raw (Parquet)
Snowpipes (auto-ingest): csv_pipe_raw (/Pos/), json_raw_pipe (/iot/), parquet_raw_pipe (/Parquet/)


Silver (GlobalMart_Silver_layer.sql)
Streams (append-only) on each raw table: stream_csv_raw, stream_json_raw, stream_parq_raw
Staging tables: stg_csv_transaction, stg_json_sensor (flattened per reading), stg_erp_parquet
Tasks (fire on SYSTEM$STREAM_HAS_DATA, insert-only MERGE):
process_csv — builds transaction_ts, floors negative qty/price/discount, recomputes line_total, normalizes payment_method; dedup on transaction_id
process_json — LATERAL FLATTEN on nested readings, pulls firmware/battery/floor from metadata; dedup on (event_id, sensor_name)
process_parquet — pass-through with timestamp; dedup on order_id


Gold (GlobalMart_Gold_layer.sql)
Dimensions: dim_store, dim_product (POS ∪ ERP-only SKUs), dim_customer, dim_supplier, dim_device
Facts: fact_sales (1 row/transaction), fact_purchase_orders (1 row/order line)
Aggregates: agg_daily_store_sales, agg_supplier_performance, agg_product_inventory_health (flags NORMAL / ORDERED_NEVER_SOLD / SOLD_NO_ORDER_RECORD / SOLD_MORE_THAN_RECEIVED), agg_customer_loyalty_summary
Note: dim_device/IoT data isn't yet joined into a Gold fact table.



Prerequisites: IAM role trusted by the storage integration with S3 access; S3 event notifications wired to each pipe's SQS queue; a running compute_wh warehouse for tasks.
