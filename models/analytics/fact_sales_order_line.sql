WITH fact_sales_order_line_source AS (
  SELECT 
    order_line_id
  , order_id 
  , stock_item_id 
  , quantity 
  , unit_price 
  , description 
  , Picking_Completed_When
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
  , cast(description as string ) as description 
  , cast(Picking_Completed_When as timestamp ) as Picking_completed_when 
  FROM fact_sales_order_line_source
)

SELECT 
  fact_line.sales_order_line_key
, fact_line.sales_order_key
, fact_line.product_key
, fact_header.customer_key
, coalesce(fact_header.picked_by_person_key,-1) as picked_by_person_key
, fact_header.order_date
, fact_header.expected_delivery_date
, fact_header.is_under_supply_back_ordered
, fact_line.quantity
, fact_line.unit_price
, fact_line.gross_amount
, fact_line.description
, fact_line.Picking_completed_when
FROM fact_sales_order_line_caculated as fact_line
LEFT JOIN {{ref('stg_fact_sales_order')}} as fact_header
  ON fact_line.sales_order_key = fact_header.sales_order_key