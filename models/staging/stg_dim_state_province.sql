with dim_province_source as (
  select * 
  from `vit-lam-data.wide_world_importers.application__state_provinces` 
)
, dim_province_rename_cast as (
SELECT DISTINCT
  cast(state_province_id as int ) as _key
  , cast(state_province_name as string ) as  _name
  , cast(country_id as int ) as  countries_key
FROM dim_province_source
)
, dim_province_final as (
SELECT  *
FROM dim_province_rename_cast
)
select 
  dim_province._key
  , dim_province._name
  , dim_countries.countries_name
from dim_province_final as dim_province
left join {{ref('stg_dim_countries')}} as dim_countries
  on dim_province.countries_key = dim_countries.countries_key
