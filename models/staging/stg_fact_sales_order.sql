with fact_sales_order_source as (
  select *
  from `vit-lam-data.wide_world_importers.sales__orders`
)
, fact_sales_order_cast_type as (
select 
  cast(order_id as int) as sales_order_key
  , cast(customer_id as int) as customer_key
  , cast(picked_by_person_id as int) as picked_by_person_key
  , cast(order_date as date ) as order_date
  , cast(expected_delivery_date as timestamp ) as expected_delivery_date
  , cast(is_undersupply_backordered as boolean ) as is_undersupply_backordered
from fact_sales_order_source
)
select 
  sales_order_key
  , customer_key
  , coalesce(picked_by_person_key,0) as picked_by_person_key
  , order_date
  , expected_delivery_date
  , is_undersupply_backordered
from fact_sales_order_cast_type
group by 1,2,3,4,5,6
