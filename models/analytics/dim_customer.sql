with customer_info_source as (
  select  
  *
  from `vit-lam-data.wide_world_importers.sales__customers`
)
, customer_info_rename as (
select 
  cast(customer_id as int ) as customer_key
  , cast(customer_name as string ) as customer_name
  , cast(customer_category_id as int ) as customer_category_key
  , cast(buying_group_id as int ) as buying_group_key
from customer_info_source
)
select 
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.customer_category_key
  , dim_customer.buying_group_key
  , dim_buying_group.buying_group_name
from customer_info_rename as dim_customer 
left join {{ ref('stg_dim_customer_category') }} as dim_customer_category 
  on dim_customer.customer_category_key = dim_customer_category.customer_category_key
left join {{ref('stg_dim_buying_group')}} as dim_buying_group 
  on dim_customer.buying_group_key = dim_buying_group.buying_group_key