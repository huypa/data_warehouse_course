with dim_membership__source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)
, dim_membership__rename_cast as (
SELECT 
    cast(customer_id  as int ) as customer_key
    , cast(membership as string ) as membership
    , cast(begin_effective_date as date ) as begin_effective_date
    , cast(end_effective_date as date ) as end_effective_date
FROM dim_membership__source
)
SELECT 
    customer_key
    , membership
    , begin_effective_date
    , end_effective_date
FROM dim_membership__rename_cast


		
	