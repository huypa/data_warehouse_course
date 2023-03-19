with dim_delivery_method_source as (
  select * 
  from `vit-lam-data.wide_world_importers.application__delivery_methods`
)
, dim_delivery_method_rename_cast as (
  select
    cast(delivery_method_id as int) as delivery_method_key
    , cast(delivery_method_name as string) as delivery_method_name
  from dim_delivery_method_source
)
select 
    delivery_method_key
    , delivery_method_name
from dim_delivery_method_rename_cast
group by 1,2