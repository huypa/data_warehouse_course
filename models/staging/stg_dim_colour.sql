with dim_colour__source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)
, dim_colour__rename_cast as (
SELECT 
    cast(color_id  as int ) as colour_key
  , cast(color_name as string ) as colour_name
FROM dim_colour__source
)
SELECT 
    colour_key
  , colour_name
FROM dim_colour__rename_cast
GROUP BY 1,2