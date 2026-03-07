-- models/marts/dim_users.sql

-- Final user dimension model
-- Combines core user profile, cleaned location attributes, and sponsor/classroom attributions
-- One row per user (with possible multiple attribution rows denormalized where applicable)

-- Base CTE: core user profile from raw source with derived fields
with users as (
  select
    id as user_id
    ,uuid
    ,first_name
    ,last_name
    ,email
    ,type as user_type
    ,case when type = 'E' then 'Educator'
         when type = 'CL' then 'Classroom Learner'
         when type = 'IL' then 'Independent Learner' end as user_type_name
    ,case when regexp_replace(lower(trim(first_name)), r'\s+', '') like '%test%'
           or regexp_replace(lower(trim(last_name)), r'\s+', '') like '%test%'
           or regexp_replace(lower(trim(email)), r'\s+', '') like '%test%'
           or email like 'educatorst1@example.com' then true else false end as is_test_user
    ,race_ethnicity
    ,case
      when lower(race_ethnicity) like '%prefer not to say%' or race_ethnicity is null then 'Prefer Not To Say'
      when ( (case when race_ethnicity like '%White%' then 1 else 0 end)
           + (case when race_ethnicity like '%Hispanic%' or race_ethnicity like '%Latinx%' then 1 else 0 end)
           + (case when race_ethnicity like '%Black%' or race_ethnicity like '%African American%' then 1 else 0 end)
           + (case when race_ethnicity like '%South Asian%' or race_ethnicity like '%East Asian%' then 1 else 0 end)
           + (case when race_ethnicity like '%Native Hawaiian or other Pacific Islander%' then 1 else 0 end)
           + (case when race_ethnicity like '%Native American or Alaska Native' then 1 else 0 end)
           + (case when race_ethnicity like '%Other%' then 1 else 0 end) ) > 1 then 'Multiracial'
      when race_ethnicity like 'Hispanic or Latinx' then 'Hispanic'
      when race_ethnicity like 'Black or African American' then 'Black'
      when race_ethnicity like '%South Asian%' or race_ethnicity like '%East Asian%' then 'Asian'
      when race_ethnicity like 'Native Hawaiian or other Pacific Islander' then 'Native Hawaiian or other Pacific Islander'
      when race_ethnicity like 'Native American or Alaska Native' then 'Native American or Alaska Native'
      when race_ethnicity like 'White' then 'White'
      when race_ethnicity like 'Other' then 'Other'
      else 'Other' end as race
    ,gender
    ,self_describe_gender
    ,case
      when gender like '%Prefer not to say%' or gender is null then 'Prefer Not To Say'
      when gender like '%Prefer to self-describe%' then 'Prefer Not To Say'
      when gender like '%Man%' and gender like '%Woman%' then 'Prefer Not To Say' -- added explicitly to handle spam user input
      when gender like '%Man%' then 'Man'
      when gender like '%Woman%' then 'Woman'
      else 'Non-binary' end as gender_sum
    ,date_joined
    ,is_active
    ,case when is_active = false then 'deactivated' else 'active' end as account_status
    ,is_staff
    ,case
      when birthday is null then null
      else date_diff(
        current_date,
        SAFE.PARSE_DATE('%Y-%m-%d', concat(substr(birthday, 4, 4), '-', substr(birthday, 1, 2), '-01')),
        year
      ) - if(format_date('%m%d', current_date) < concat(substr(birthday, 1, 2), '01'), 1, 0) end as age
    ,location_id
  from {{ source('raw','user_core') }}
)
-- Enriched final output: one row per user with optional sponsor/classroom/site details
select
  users.*
  ,locations.country
  ,locations.state
  ,locations.county
  ,locations.city
  ,locations.city_latitude
  ,locations.city_longitude
  ,attributions.sponsor_id
  ,case
    when users.user_type = 'IL' then null
    when attributions.sponsor_name is null then null
    else attributions.sponsor_name end as sponsor_name
  ,attributions.sponsor_code
  ,attributions.classroom_id
  ,attributions.classroom_name
  ,attributions.classroom_code
  ,attributions.site_id
  ,attributions.site_name
from users
left join {{ ref('int_user_attributions') }} as attributions on users.user_id = attributions.user_id
left join {{ ref('int_locations_clean') }} as locations on users.location_id = locations.from_location_id
order by users.user_id, attributions.sponsor_id, attributions.classroom_id, attributions.site_id asc
