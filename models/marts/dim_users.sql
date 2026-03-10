-- models/marts/dim_users.sql
--
-- Final user dimension model
-- Combines core user profile, cleaned location attributes, and sponsor/classroom attributions
-- One row per user_id (Kimball dimension best practice: single version of truth)
-- Deduplicates using most recent date_joined, preferring active accounts and non-null values

WITH users AS (
  SELECT
    id AS user_id
    ,uuid
    ,first_name
    ,last_name
    ,email
    ,type AS user_type
    -- user_type is a drop down and can only contain 'E','CL', or 'IL', or null
    ,CASE
      WHEN type = 'E' THEN 'Educator'
      WHEN type = 'CL' THEN 'Classroom Learner'
      WHEN type = 'IL' THEN 'Independent Learner'
    END AS user_type_name
    ,CASE
      WHEN REGEXP_REPLACE(LOWER(TRIM(first_name)), r'\s+', '') LIKE '%test%'
        OR REGEXP_REPLACE(LOWER(TRIM(last_name)), r'\s+', '') LIKE '%test%'
        OR REGEXP_REPLACE(LOWER(TRIM(email)), r'\s+', '') LIKE '%test%'
        OR email LIKE 'educatorst1@example.com' THEN TRUE
      ELSE FALSE
    END AS is_test_user
    ,race_ethnicity
    -- race_ethnicity is a drop-down so we know what inputs to expect
    ,CASE
      WHEN LOWER(race_ethnicity) LIKE '%prefer not to say%' OR race_ethnicity IS NULL THEN 'Prefer Not To Say'
      WHEN (CASE WHEN race_ethnicity LIKE '%White%' THEN 1 ELSE 0 END +
           CASE WHEN race_ethnicity LIKE '%Hispanic%' OR race_ethnicity LIKE '%Latinx%' THEN 1 ELSE 0 END +
           CASE WHEN race_ethnicity LIKE '%Black%' OR race_ethnicity LIKE '%African American%' THEN 1 ELSE 0 END +
           CASE WHEN race_ethnicity LIKE '%South Asian%' OR race_ethnicity LIKE '%East Asian%' THEN 1 ELSE 0 END +
           CASE WHEN race_ethnicity LIKE '%Native Hawaiian or other Pacific Islander%' THEN 1 ELSE 0 END +
           CASE WHEN race_ethnicity LIKE '%Native American or Alaska Native%' THEN 1 ELSE 0 END +
           CASE WHEN race_ethnicity LIKE '%Other%' THEN 1 ELSE 0 END) > 1 THEN 'Multiracial'
      WHEN race_ethnicity LIKE 'Hispanic or Latinx' THEN 'Hispanic'
      WHEN race_ethnicity LIKE 'Black or African American' THEN 'Black'
      WHEN race_ethnicity LIKE '%South Asian%' OR race_ethnicity LIKE '%East Asian%' THEN 'Asian'
      WHEN race_ethnicity LIKE 'Native Hawaiian or other Pacific Islander' THEN 'Native Hawaiian or other Pacific Islander'
      WHEN race_ethnicity LIKE 'Native American or Alaska Native' THEN 'Native American or Alaska Native'
      WHEN race_ethnicity LIKE 'White' THEN 'White'
      WHEN race_ethnicity LIKE 'Other' THEN 'Other'
      ELSE 'Other'
    END AS race
    ,gender
    ,self_describe_gender
    -- gender is a multi-select drop-down so only contains exact inputs we search for here:
    ,CASE
      WHEN gender LIKE 'Prefer not to say' OR gender IS NULL THEN 'Prefer Not To Say'
      WHEN gender LIKE '%Prefer to self-describe%' THEN 'Non-binary'
      WHEN gender LIKE '%Man%' AND gender LIKE '%Woman%' THEN 'Non-binary'
      WHEN gender LIKE 'Non-binary' THEN 'Non-binary'
      WHEN gender LIKE 'Man' THEN 'Man'
      WHEN gender LIKE 'Woman' THEN 'Woman'
      ELSE 'Prefer Not To Say'
    END AS gender_sum
    ,date_joined
    ,is_active
    ,CASE WHEN is_active = FALSE THEN 'deactivated' ELSE 'active' END AS account_status
    ,is_staff
    ,CASE
      WHEN birthday IS NULL THEN NULL
      ELSE DATE_DIFF(CURRENT_DATE(), birthday, YEAR)
           - IF(
               DATE_TRUNC(CURRENT_DATE(), MONTH) < DATE_TRUNC(birthday, MONTH)
               OR (DATE_TRUNC(CURRENT_DATE(), MONTH) = DATE_TRUNC(birthday, MONTH)
                   AND EXTRACT(DAY FROM CURRENT_DATE()) < EXTRACT(DAY FROM birthday)),
               1,
               0
             )
    END AS age
    ,location_id
  FROM {{ source('raw', 'user_core') }}
)

-- Join enriched location hierarchy and attributions
, enriched AS (
  SELECT
    users.*
    ,locations.country
    ,locations.state
    ,locations.county
    ,locations.city
    ,locations.city_latitude
    ,locations.city_longitude
    ,attributions.sponsor_id
    ,CASE
      WHEN users.user_type = 'IL' THEN NULL
      WHEN attributions.sponsor_name IS NULL THEN NULL
      ELSE attributions.sponsor_name
    END AS sponsor_name
    ,attributions.sponsor_code
    ,attributions.classroom_id
    ,attributions.classroom_name
    ,attributions.classroom_code
    ,attributions.site_id
    ,attributions.site_name
  FROM users
  LEFT JOIN {{ ref('int_locations_clean') }} AS locations
    ON users.location_id = locations.from_location_id
  LEFT JOIN {{ ref('int_user_attributions') }} AS attributions
    ON users.user_id = attributions.user_id
)

-- Deduplicate to ensure one row per user_id
-- Priority: most recent join date, then active accounts, then non-null values
,ranked_users AS (
  SELECT
    *
    ,ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY
        date_joined DESC NULLS LAST           -- most recent join first
        ,is_active DESC NULLS LAST            -- prefer active accounts
        ,email DESC NULLS LAST                -- prefer non-null email
        ,first_name DESC NULLS LAST           -- tie-breaker
    ) AS rn
  FROM enriched
)

-- Final output: single row per user
SELECT *
FROM ranked_users
WHERE rn = 1