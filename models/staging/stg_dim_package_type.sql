with dim_package_type_source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)
, dim_package_type_rename_cast as (
SELECT 
  cast(package_type_id  as int ) as package_type_key
  , cast(package_type_name as string ) as package_type_name
FROM dim_package_type_source
)
SELECT 
  package_type_key
  , package_type_name
FROM dim_package_type_rename_cast
GROUP BY 1,2