-----------------------------------------------------------------------------
-- GOLD LAYER  --  global_mart_db.gold
-- Follows the Galaxy Schema 
----- dim_store --------------------------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.dim_store AS
SELECT
    ROW_NUMBER() OVER (ORDER BY store_id)  AS store_key,
    store_id,
    store_name,
    store_city,
    store_region
FROM (
    SELECT
        store_id,
        store_name,
        store_city,
        store_region
    FROM global_mart_db.staging.stg_csv_transaction
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY load_timestamp DESC
    ) = 1
);
----- dim_product ------------------------------------    -- POS side has -product_name  -subcategory
CREATE OR REPLACE TABLE global_mart_db.gold.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_sku)  AS product_key,
    product_sku,
    product_name,
    category,
    subcategory
FROM (
    SELECT DISTINCT
        product_sku,
        product_name,
        category,
        subcategory
    FROM global_mart_db.staging.stg_csv_transaction

    UNION
    SELECT DISTINCT
        product_sku,
        NULL AS product_name,
        category,
        NULL AS subcategory
    FROM global_mart_db.staging.stg_erp_parquet
    WHERE product_sku NOT IN (SELECT DISTINCT product_sku FROM global_mart_db.staging.stg_csv_transaction)
);
select * from GLOBAL_MART_DB.GOLD.DIM_PRODUCT; ---- some values are null thats mean those products are not sold 


--- dim_customer ---------------------------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id)  AS customer_key,
    customer_id,
    first_seen_date,
    last_seen_date,
    total_loyalty_points
FROM (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_seen_date,
        MAX(transaction_date) AS last_seen_date,
        SUM(loyalty_points)   AS total_loyalty_points
    FROM global_mart_db.staging.stg_csv_transaction
    GROUP BY customer_id
);
select * from GLOBAL_MART_DB.GOLD.dim_customer;
----- dim_supplier ---------------------------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.dim_supplier AS
SELECT
    ROW_NUMBER() OVER (ORDER BY supplier_id)  AS supplier_key,
    supplier_id,
    supplier_name,
    supplier_city
FROM (
    SELECT DISTINCT
        supplier_id,
        supplier_name,
        supplier_city
    FROM global_mart_db.staging.stg_erp_parquet
);
----- dim_device ---------------------------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.dim_device AS
SELECT
    ROW_NUMBER() OVER (ORDER BY device_id)  AS device_key,
    device_id,
    store_id,
    store_floor,
    firmware
FROM (
    SELECT DISTINCT
        device_id,
        store_id,
        store_floor,
        firmware
    FROM global_mart_db.staging.stg_json_sensor
);
select  * from global_mart_db.gold.dim_device;

----- fact_sales------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.fact_sales AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.transaction_id)  AS sales_key,
    t.transaction_id,
    t.transaction_date,
    s.store_key,
    p.product_key,
    c.customer_key,
    t.cashier_id,
    t.transaction_ts,
    t.quantity,
    t.unit_price,
    t.discount_pct,
    t.line_total,
    t.payment_method,
    t.loyalty_points
FROM global_mart_db.staging.stg_csv_transaction t
LEFT JOIN global_mart_db.gold.dim_store    s ON t.store_id = s.store_id and t.store_city= s.store_city
LEFT JOIN global_mart_db.gold.dim_product  p ON t.product_sku = p.product_sku
LEFT JOIN global_mart_db.gold.dim_customer c ON t.customer_id = c.customer_id;

select * from global_mart_db.gold.fact_sales;

----- fact_purchase_orders (grain: 1 row per ERP order line) --------------------
CREATE OR REPLACE TABLE global_mart_db.gold.fact_purchase_orders AS
SELECT
    ROW_NUMBER() OVER (ORDER BY o.order_id)  AS po_key,
    o.order_id,
    o.order_date,
    s.store_key,
    sup.supplier_key,
    p.product_key,
    o.quantity_ordered,
    o.quantity_received,
    o.unit_cost,
    o.total_cost,
    o.order_status,
    o.expected_delivery,
    o.actual_delivery,
    o.warehouse_id,
    o.lead_time_days,
    o.is_late
FROM global_mart_db.staging.stg_erp_parquet o
LEFT JOIN global_mart_db.gold.dim_store    s   ON o.store_id = s.store_id and o.store_city= s.store_city
LEFT JOIN global_mart_db.gold.dim_supplier sup ON o.supplier_id = sup.supplier_id
LEFT JOIN global_mart_db.gold.dim_product  p   ON o.product_sku = p.product_sku;

select * from global_mart_db.gold.fact_purchase_orders;

----- agg_daily_store_sales ---------------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.agg_daily_store_sales AS
SELECT
    f.transaction_date,
    f.store_key,
    SUM(f.line_total)                                            AS total_revenue,
    SUM(f.quantity)                                               AS total_units_sold,
    COUNT(DISTINCT f.transaction_id)                              AS total_transactions,
    SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.transaction_id), 0) AS avg_basket_size
FROM global_mart_db.gold.fact_sales f
GROUP BY f.transaction_date, f.store_key;
select * from global_mart_db.gold.agg_daily_store_sales;



----- agg_supplier_performance --------------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.agg_supplier_performance AS
SELECT
    f.supplier_key,
    COUNT(*)                                                  AS total_orders,
    SUM(CASE WHEN f.is_late = FALSE THEN 1 ELSE 0 END)        AS on_time_orders,
    SUM(CASE WHEN f.is_late = FALSE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS on_time_pct,
    AVG(f.lead_time_days)                                     AS avg_lead_time_days,
    AVG(f.total_cost - (f.unit_cost * f.quantity_received))   AS avg_cost_variance
FROM global_mart_db.gold.fact_purchase_orders f
GROUP BY f.supplier_key;
select * from global_mart_db.gold.agg_supplier_performance;


-------agg_product_inventory_health---------------------------------------------------------------------------

CREATE OR REPLACE TABLE global_mart_db.gold.agg_product_inventory_health AS
WITH po_agg AS (
    SELECT
        product_key,
        store_key,
        SUM(quantity_ordered)  AS total_qty_ordered,
        SUM(quantity_received) AS total_qty_received
    FROM global_mart_db.gold.fact_purchase_orders
    GROUP BY product_key, store_key
),
sales_agg AS (
    SELECT
        product_key,
        store_key,
        SUM(quantity) AS total_qty_sold
    FROM global_mart_db.gold.fact_sales
    GROUP BY product_key, store_key
),
all_combos AS (
    SELECT product_key, store_key FROM po_agg
    UNION
    SELECT product_key, store_key FROM sales_agg
)
SELECT
    c.product_key,
    c.store_key,
    COALESCE(po.total_qty_ordered, 0)  AS total_qty_ordered,
    COALESCE(po.total_qty_received, 0) AS total_qty_received,
    COALESCE(s.total_qty_sold, 0)      AS total_qty_sold,
    CASE
        WHEN COALESCE(po.total_qty_ordered, 0) = 0 AND COALESCE(s.total_qty_sold, 0) > 0 THEN 'SOLD_NO_ORDER_RECORD'
        WHEN COALESCE(s.total_qty_sold, 0) = 0 AND COALESCE(po.total_qty_ordered, 0) > 0 THEN 'ORDERED_NEVER_SOLD'
        WHEN COALESCE(s.total_qty_sold, 0) > COALESCE(po.total_qty_received, 0) THEN 'SOLD_MORE_THAN_RECEIVED'
        ELSE 'NORMAL'
    END AS stock_health_flag
FROM all_combos c
LEFT JOIN po_agg    po ON c.product_key = po.product_key AND c.store_key = po.store_key
LEFT JOIN sales_agg s  ON c.product_key = s.product_key  AND c.store_key = s.store_key;

select * from global_mart_db.gold.agg_product_inventory_health ;

----- agg_customer_loyalty_summary -----------------------------------------------
CREATE OR REPLACE TABLE global_mart_db.gold.agg_customer_loyalty_summary AS
SELECT
    f.customer_key,
    SUM(f.line_total)                                             AS total_spend,
    COUNT(DISTINCT f.transaction_id)                              AS total_visits,
    SUM(f.loyalty_points)                                         AS total_loyalty_points,
    SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.transaction_id), 0) AS avg_spend_per_visit
FROM global_mart_db.gold.fact_sales f
GROUP BY f.customer_key;
select * from global_mart_db.gold.agg_customer_loyalty_summary;




-----------------------------------------------------
SELECT * FROM global_mart_db.gold.dim_store;
SELECT * FROM global_mart_db.gold.dim_product;
SELECT * FROM global_mart_db.gold.dim_customer;
SELECT * FROM global_mart_db.gold.dim_supplier;
SELECT * FROM global_mart_db.gold.dim_device;
-------------------------------------------------------
SELECT * FROM global_mart_db.gold.fact_sales;
SELECT * FROM global_mart_db.gold.fact_purchase_orders;
--------------------------------------------------------
SELECT * FROM global_mart_db.gold.agg_daily_store_sales;
SELECT * FROM global_mart_db.gold.agg_supplier_performance;
SELECT * FROM global_mart_db.gold.agg_product_inventory_health;
SELECT * FROM global_mart_db.gold.agg_customer_loyalty_summary;
