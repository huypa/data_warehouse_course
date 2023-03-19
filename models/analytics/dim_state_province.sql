with dim__source as (
  select * 
  from `vit-lam-data.wide_world_importers.application__state_s` 
)
, dim__rename_cast as (
SELECT DISTINCT
  cast(state__id as int ) as _key
  , cast(state__name as string ) as  _name
  , cast(country_id as int ) as  countries_key
FROM dim__source
)
, dim__final as (
SELECT  *
FROM dim__rename_cast
)
select 
  dim_._key
  , dim_._name
  , dim_countries.countries_name
from dim__final as dim_
left join {{ref('dim_countries')}} as dim_countries
  on dim_.countries_key = dim_countries.countries_key
