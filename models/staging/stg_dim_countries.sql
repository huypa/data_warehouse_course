with dim_countries__source as (
  select * 
  from `vit-lam-data.wide_world_importers.application__countries` 
)
, dim_countries__rename_cast as (
SELECT DISTINCT
  cast(country_id as int ) as  countries_key
  , cast(Country_Name as string ) as  countries_name
FROM dim_countries__source
)
, dim_countries__final as (
SELECT  *
FROM dim_countries__rename_cast
)
select 
  dim_countries.countries_key
  , dim_countries.countries_name
from dim_countries__final as dim_countries
