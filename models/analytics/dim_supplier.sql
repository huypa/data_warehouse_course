with dim_supplier_source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)
, dim_supplier_rename_cast as (
SELECT 
    cast(supplier_id  as int ) as supplier_key
  , cast(supplier_name as string ) as supplier_name
FROM dim_supplier_source
)
SELECT 
    supplier_key
  , supplier_name
FROM dim_supplier_rename_cast
GROUP BY 1,2