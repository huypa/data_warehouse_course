with customer_info_source as (
select  
  customer_id
, customer_name
from `vit-lam-data.wide_world_importers.sales__customers`
group by 1,2
)
, customer_info_rename as (
select 
  customer_id as customer_key
, customer_name as customer_name
from customer_info_source
)
select 
  customer_key
, customer_name
from customer_info_rename