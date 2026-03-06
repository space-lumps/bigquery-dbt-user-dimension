-- models/intermediate/int_locations_clean.sql

-- Intermediate model: Resolves the most reliable city/state/country/location coordinates per location ID
-- Used downstream in dim_users for user location enrichment

with location_components as (
  select
    location_address_components.from_location_id
    ,to_location.id as to_location_id
    ,to_location.display_name
    ,to_location.long_name
    ,to_location.latitude
    ,to_location.longitude
    ,location_type.locationtype_id
  from {{ source('raw','location_address_components') }}
  join {{ source('raw','location_core') }} as to_location on location_address_components.to_location_id = to_location.id
  join {{ source('raw','location_type') }} on location_address_components.to_location_id = location_type.location_id
  where location_type.locationtype_id in (1,3,4,7,8)

  union all

  select
    from_location.id as from_location_id
    ,from_location.id as to_location_id
    ,from_location.display_name
    ,from_location.long_name
    ,from_location.latitude
    ,from_location.longitude
    ,location_type.locationtype_id
  from {{ source('raw','location_core') }} as from_location
  join {{ source('raw','location_type') }} as location_type on from_location.id = location_type.location_id
  where location_type.locationtype_id in (1,3,4,7,8)
)
, multiple as (
  select from_location_id
  from location_components
  where locationtype_id in (3,4)
    and from_location_id != to_location_id
  group by from_location_id
  having count(distinct display_name) > 1
)
, city_candidates as (
  select
    from_location.id as from_location_id
    ,city.to_location_id
    ,city.display_name as city
    ,city.latitude as city_latitude
    ,city.longitude as city_longitude
    ,from_location.display_name as original_locale
    ,from_location.latitude as og_latitude
    ,from_location.longitude as og_longitude
    ,st_distance(st_geogpoint(city.longitude, city.latitude), st_geogpoint(from_location.longitude, from_location.latitude)) / 1609.34 as distance_from_origin
  from multiple
  join {{ source('raw','location_core') }} as from_location on from_location.id = multiple.from_location_id
  join location_components as city on city.from_location_id = from_location.id and city.locationtype_id in (3,4)
  where from_location.latitude is not null
    and from_location.longitude is not null
    and city.latitude is not null
    and city.longitude is not null
)
, replace as (
  select
    from_location_id
    ,to_location_id
    ,original_locale
    ,distance_from_origin
    ,case when distance_from_origin > 10
           and not regexp_contains(lower(original_locale), r'\d{1,5}\s+\w+')
           and not regexp_contains(lower(original_locale), r'^[0-9a-z]{4}\+[0-9a-z]{2,}')
           and not regexp_contains(lower(original_locale), r'\bst\b|\bave\b|\brd\b|\bdr\b|\bln\b')
           and not lower(original_locale) like '%county%'
         then original_locale else city end as city_replace
    ,case when distance_from_origin > 10 then og_latitude else city_latitude end as city_latitude_replace
    ,case when distance_from_origin > 10 then og_longitude else city_longitude end as city_longitude_replace
    ,og_latitude
    ,og_longitude
  from (
    select *, row_number() over (partition by from_location_id order by distance_from_origin) as rn
    from city_candidates
  ) ranked
  where rn = 1
)
, best_country as (
  select *
  from location_components
  where locationtype_id = 1
  qualify row_number() over (partition by from_location_id order by to_location_id) = 1
)
, best_state as (
  select *
  from location_components
  where locationtype_id = 7
  qualify row_number() over (partition by from_location_id order by to_location_id) = 1
)
, best_county as (
  select *
  from location_components
  where locationtype_id = 8
  qualify row_number() over (partition by from_location_id order by to_location_id) = 1
)
, all_location_flat as (
  select
    from_location.id as from_location_id
    ,from_location.display_name as original_locale
    ,case
      when replace.city_replace is not null then replace.city_replace
      when location_type.locationtype_id = 1 then null
      when best_city.display_name is not null then best_city.display_name
      when from_location.display_name = 'Seoul' then from_location.display_name
      else null end as city
    ,coalesce(replace.city_latitude_replace, best_city.latitude, from_location.latitude) as city_latitude
    ,coalesce(replace.city_longitude_replace, best_city.longitude, from_location.longitude) as city_longitude
    ,best_county.display_name as county
    ,case
      when location_type.locationtype_id = 1 then null
      when best_state.display_name = best_country.long_name then null
      when best_state.display_name is not null then best_state.display_name
      else null end as state
    ,best_country.long_name as country
    ,from_location.latitude
    ,from_location.longitude
    ,from_location.slug
    ,location_type.locationtype_id
  from {{ source('raw','location_core') }} as from_location
  left join replace on from_location.id = replace.from_location_id
  left join best_county on from_location.id = best_county.from_location_id
  left join best_state on from_location.id = best_state.from_location_id
  left join best_country on from_location.id = best_country.from_location_id
  left join {{ source('raw','location_type') }} on from_location.id = location_type.location_id
  left join (
    select * except(rn)
    from (
      select *, row_number() over (partition by from_location_id order by to_location_id) as rn
      from location_components
      where locationtype_id in (3,4)
    ) city_ranked
    where rn = 1
  ) as best_city on from_location.id = best_city.from_location_id
)
, all_location_flat_scored as (
  select
    *
    ,(case when city is not null then 1 else 0 end
     + case when city_latitude is not null then 1 else 0 end
     + case when city_longitude is not null then 1 else 0 end
     + case when county is not null then 1 else 0 end
     + case when state is not null then 1 else 0 end
     + case when country is not null then 1 else 0 end
     + case when latitude is not null then 1 else 0 end
     + case when longitude is not null then 1 else 0 end) as score
  from all_location_flat
  where latitude is not null and longitude is not null
)
, all_location_flat_deduped as (
  select * except(score)
  from (
    select *, row_number() over (partition by from_location_id order by score desc) as row_rank
    from all_location_flat_scored
  ) ranked
  where row_rank = 1
)

select
  from_location_id
  ,original_locale
  ,city
  ,county
  ,state
  ,country
  ,city_latitude
  ,city_longitude
from all_location_flat_deduped
