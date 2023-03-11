with fact_sales_order_source as (
  select *
  from `vit-lam-data.wide_world_importers.sales__orders`
)
, fact_sales_order_cast_type as (
select 
  cast(order_id as int) as sales_order_key
  , cast(customer_id as int) as customer_key
  , cast(picked_by_person_id as int) as picked_by_person_key
from fact_sales_order_source
)
select 
  sales_order_key
  , customer_key
  , picked_by_person_key
from fact_sales_order_cast_type
group by 1,2
