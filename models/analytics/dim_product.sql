with dim_product_source as (
  select * 
  from `vit-lam-data.wide_world_importers.warehouse__stock_items` 
)
, dim_product_rename_cast as (
SELECT  
  cast(stock_item_id as int ) as product_key
  , cast(stock_item_name as string ) as  product_name
  , cast(brand as string ) as  brand_name 
  , cast(size as string ) as  size 
  , cast(supplier_id as int) as supplier_key
  , cast(is_chiller_stock as boolean) as is_chiller_stock
  , cast(unit_price as float64) as unit_price
  , cast(quantity_per_outer as int) as quantity_per_outer
  , cast(typical_weight_per_unit as float64) as typical_weight_per_unit
  , cast(barcode as string) as bar_code
  , cast(lead_time_days as int) as lead_time_days  
FROM dim_product_source
)
, dim_product_final as (
  SELECT  
  product_key
  , product_name
  , brand_name 
  , supplier_key
  , case when is_chiller_stock is true then "Chiller_stock"
         when is_chiller_stock is false then "Non chillder stock"
         when is_chiller_stock is null then "Undefined"
         else "Invalid"
    end as is_chiller_stock -- sua chiller thanh string
FROM dim_product_rename_cast
)
select 
  dim_product.Product_key
  , dim_product.Product_name
  , coalesce(dim_product.brand_name,"Undefined") as Brand_name
  , dim_product.Size
  , dim_product.Is_chiller_stock
  , dim_product.Unit_price
  , dim_product.Quantity_per_outer
  , dim_product.Typical_weight_per_unit
  , dim_product.Bar_code
  , dim_product.Lead_time_days
  , coalesce(dim_supplier.supplier_name,"Invalid") as Supplier_name
  , dim_supplier_category.Supplier_category_key
  , dim_supplier_category.Supplier_category_name
  , dim_supplier.Delivery_method_key
  , dim_supplier.Delivery_method_name 
  , dim_supplier.Delivery_city_key 
  , dim_supplier.Delivery_city_name 
  , dim_supplier.Delivery_province_name
  , dim_supplier.Delivery_countries_name
from dim_product_final as dim_product
left join {{ref('dim_supplier')}} as dim_supplier 
  on dim_product.supplier_key = dim_supplier.supplier_key
