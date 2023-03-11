with dim_product_source as (
  select * 
  from `vit-lam-data.wide_world_importers.warehouse__stock_items` 
)
, dim_product_rename_cast as (
SELECT  
  cast(stock_item_id as int ) as product_key
, cast(stock_item_name as string ) as  product_name
, cast(brand as string ) as  brand_name 
, cast(supplier_id as string) as supplier_key
FROM dim_product_source
)
select 
    dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_supplier.supplier_key
from dim_product_rename_cast as dim_product
left join dim_product_rename_cast as dim_supplier 
  on dim_product.supplier_key = dim_supplier.supplier_key
