WITH fact_sales_order_line_source AS (
  SELECT 
    order_line_id
  , order_id 
  , stock_item_id 
  , quantity 
  , unit_price 
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line_caculated as (
  SELECT 
    cast(order_line_id as int) as sales_order_line_key
  , cast(order_id as int) as sales_order_key
  , cast(stock_item_id as int) as product_key
  , cast(quantity as int) as quantity
  , cast(unit_price as numeric ) as unit_price
  --, cast((quantity * unit_price) as numeric ) as gross_amount
  , cast(quantity as int) * cast(unit_price as numeric) as gross_amount
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

SELECT 
  fact_line.sales_order_line_key
, fact_line.sales_order_key
, fact_line.product_key
, fact_header.customer_key
, fact_line.quantity
, fact_line.unit_price
, fact_line.gross_amount
FROM fact_sales_order_line_caculated as fact_line
LEFT JOIN `learn-dbt-379208.wide_world_importers_dwh_staging.stg_fact_sales_order_line` as fact_header
  ON fact_line.sales_order_key = fact_header.sales_order_key