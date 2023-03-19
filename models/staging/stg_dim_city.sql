with dim_city_source as (
  select * 
  from `vit-lam-data.wide_world_importers.application__cities` 
)
, dim_city_rename_cast as (
SELECT DISTINCT
  cast(city_id as int ) as city_key
  , cast(city_name as string ) as  city_name
  , cast(state_province_id as int ) as province_key
FROM dim_city_source
)
, dim_city_final as (
SELECT  *
FROM dim_city_rename_cast
)
select 
  dim_city.city_key
  , dim_city.city_name
  , dim_province.province_name
  , dim_province.countries_name
from dim_city_final as dim_city
left join {{ref('stg_dim_state_province')}} as dim_province
  on dim_city.province_key = dim_province.province_key