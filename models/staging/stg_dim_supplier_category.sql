with dim_supplier_categories__source as (
  select * 
  from `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)
, dim_supplier_categories__rename_cast as (
  select
    cast(supplier_category_id as int) as supplier_category_key
    , cast(supplier_category_name as string) as supplier_category_name
  from dim_supplier_categories__source
)
select 
    supplier_category_key
    , supplier_category_name
from dim_supplier_categories__rename_cast
group by 1,2