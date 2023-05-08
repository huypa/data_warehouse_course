with dim_external_category__source as (
    SELECT 
      *
    FROM `vit-lam-data.wide_world_importers.external__categories`
)
, dim_external_category__rename_cast as (
    SELECT 
      cast(category_id  as int ) as category_key
      , cast(category_name as string ) as category_name
      , cast(parent_category_id  as int ) as parent_category_key
      , cast(category_level  as int ) as category_level
    FROM dim_external_category__source
    union all 
    SELECT 
        0 as category_key
      , "Undefined" as category_name
      , 0 as parent_category_key
      , 0 as category_level
    union all 
    SELECT 
        -1 as category_key
      , "Error" as category_name
      , -1 as parent_category_key
      , -1 as category_level
)
SELECT 
    category_key
    , category_name
    , parent_category_key
    , category_level
FROM dim_external_category__rename_cast
GROUP BY 1,2