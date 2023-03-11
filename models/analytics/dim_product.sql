with dim_product_source as (
  select * 
  from `vit-lam-data.wide_world_importers.warehouse__stock_items` 
)
, dim_product_rename_cast as (
SELECT  
  cast(stock_item_id as int ) as product_key
  , cast(stock_item_name as string ) as  product_name
  , cast(brand as string ) as  brand_name 
  , cast(supplier_id as int) as supplier_key
  , cast(is_chiller_stock as boolean) as is_chiller_stock
FROM dim_product_source
)
, dim_product_final as (
  SELECT  
  product_key
  , product_name
  , brand_name 
  , supplier_key
  , is_chiller_stock
  , case when is_chiller_stock is true then "Chiller_stock"
         when is_chiller_stock is false then "Non chillder stock"
         when is_chiller_stock is null then "Undefined"
         else "Invalid"
    end as is_chiller_stock -- sua chiller thanh string
FROM dim_product_source
)
select 
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.is_chiller_stock
  , dim_product.supplier_key
  , dim_supplier.supplier_name
from dim_product_final as dim_product
left join {{ref('dim_supplier')}} as dim_supplier 
  on dim_product.supplier_key = dim_supplier.supplier_key
