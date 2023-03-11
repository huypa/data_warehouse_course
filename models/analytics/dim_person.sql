with dim_person_source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.application__people`
)
, dim_person_rename_cast as (
SELECT 
    cast(person_id  as int ) as person_key
  , cast(full_name as string ) as full_name
FROM dim_person_source
union all 
select 
    0 as person_key
  , "Undefined" as full_name

union all 
select 
    -1 as person_key
  , "Error" as full_name
)
SELECT 
    person_key
  , full_name
FROM dim_person_rename_cast
GROUP BY 1,2