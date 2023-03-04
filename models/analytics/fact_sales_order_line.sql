WITH fact_sales_order_line_source AS (
  SELECT 
    order_line_id 
  , stock_item_id 
  , quantity 
  , unit_price 
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line_rename as (
  SELECT 
    cast(order_line_id as int) as sales_order_line_key
  , cast(stock_item_id as int) as product_key
  , cast(quantity as int) as quantity
  , cast(unit_price as numeric ) as unit_price
  --, cast((quantity * unit_price) as numeric ) as gross_amount
  , cast(quantity as int) * cast(unit_price as numeric) as gross_amount
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

SELECT 
  sales_order_line_key
, product_key
, quantity
, unit_price
, gross_amount
FROM fact_sales_order_line_rename
