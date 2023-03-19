with dim_colour_source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)
, dim_package_type_rename_cast as (
SELECT 
    cast(colour_id  as int ) as colour_key
  , cast(colour_name as string ) as colour_name
FROM dim_colour_source
)
SELECT 
    colour_key
  , colour_name
FROM dim_colour_rename_cast
GROUP BY 1,2