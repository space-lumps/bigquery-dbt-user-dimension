-- models/intermediate/int_user_attributions.sql
--
-- Intermediate model: Unifies multiple user attribution paths into one grain
-- Captures four distinct onboarding/join routes: classroom membership (learner/educator),
-- classroom invitation (email match), and direct sponsor invite code join.
-- Output can have multiple rows per user_id (one per attribution path).
-- Downstream dim_users.sql deduplicates and selects canonical values.

WITH attributions AS (
  -- Route 1: Learners via classroom → site → sponsor
  SELECT
    NULL AS educator_id
    ,learner_classroom_membership.user_id AS learner_id
    ,classroom.site_id
    ,user_site.name AS site_name
    ,user_sponsor.id AS sponsor_id
    ,user_sponsor.name AS sponsor_name
    ,sponsor_invite_code.code AS sponsor_code
    ,classroom.id AS classroom_id
    ,classroom.name AS classroom_name
    ,classroom_invite_code.code AS classroom_code
  FROM {{ source('raw','learner_classroom_membership') }}
  LEFT JOIN {{ source('raw','classroom') }} ON classroom.id = learner_classroom_membership.classroom_id
  LEFT JOIN {{ source('raw','user_site') }} ON classroom.site_id = user_site.id
  LEFT JOIN {{ source('raw','user_sponsor') }} ON user_site.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','sponsor_invite_code') }} ON sponsor_invite_code.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','classroom_invite_code') }} ON classroom_invite_code.classroom_id = classroom.id

  UNION ALL

  -- Route 2: Educators via classroom → site → sponsor
  SELECT
    educator_classroom_membership.user_id AS educator_id
    ,NULL AS learner_id
    ,classroom.site_id
    ,user_site.name AS site_name
    ,user_sponsor.id AS sponsor_id
    ,user_sponsor.name AS sponsor_name
    ,sponsor_invite_code.code AS sponsor_code
    ,classroom.id AS classroom_id
    ,classroom.name AS classroom_name
    ,classroom_invite_code.code AS classroom_code
  FROM {{ source('raw','educator_classroom_membership') }}
  LEFT JOIN {{ source('raw','classroom') }} ON educator_classroom_membership.classroom_id = classroom.id
  LEFT JOIN {{ source('raw','user_site') }} ON classroom.site_id = user_site.id
  LEFT JOIN {{ source('raw','user_sponsor') }} ON user_site.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','sponsor_invite_code') }} ON sponsor_invite_code.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','classroom_invite_code') }} ON classroom_invite_code.classroom_id = classroom.id

  UNION ALL

  -- Route 3: Learners invited via classroom invitation (matched by email)
  SELECT
    NULL AS educator_id
    ,user_core.id AS learner_id
    ,classroom.site_id
    ,user_site.name AS site_name
    ,user_sponsor.id AS sponsor_id
    ,user_sponsor.name AS sponsor_name
    ,sponsor_invite_code.code AS sponsor_code
    ,classroom.id AS classroom_id
    ,classroom.name AS classroom_name
    ,classroom_invite_code.code AS classroom_code
  FROM {{ source('raw','educator_invitation_record') }}
  JOIN {{ source('raw','user_core') }} 
    ON lower(trim(user_core.email)) = lower(trim(educator_invitation_record.email)) 
    AND user_core.type != 'IL' -- Exclude Independent Learners AS they don't use invitations
  join {{ source('raw','classroom') }} ON classroom.id = educator_invitation_record.classroom_id
  LEFT JOIN {{ source('raw','user_site') }} ON classroom.site_id = user_site.id
  LEFT JOIN {{ source('raw','user_sponsor') }} ON user_site.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','sponsor_invite_code') }} ON sponsor_invite_code.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','classroom_invite_code') }} ON classroom_invite_code.classroom_id = classroom.id

  UNION ALL

  -- Route 4: Learners who joined via sponsor invite code
  SELECT
    NULL AS educator_id
    ,user_core.id AS learner_id
    ,sponsor_invite_code.site_id
    ,user_site.name AS site_name
    ,user_sponsor.id AS sponsor_id
    ,user_sponsor.name AS sponsor_name
    ,sponsor_invite_code.code AS sponsor_code
    ,NULL AS classroom_id
    ,NULL AS classroom_name
    ,NULL AS classroom_code
  FROM {{ source('raw','user_join_record') }}
  JOIN {{ source('raw','user_core') }} 
    ON user_core.id = user_join_record.user_id 
    AND user_core.type != 'IL' -- Exclude Independent Learners
  JOIN {{ source('raw','sponsor_invite_code') }} ON user_join_record.sponsor_invite_code_id = sponsor_invite_code.id
  LEFT JOIN {{ source('raw','user_sponsor') }} ON sponsor_invite_code.sponsor_id = user_sponsor.id
  LEFT JOIN {{ source('raw','user_site') }} ON sponsor_invite_code.site_id = user_site.id
  WHERE user_join_record.action_type = 'userjoins'
)
-- Stack educator and learner attributions, deduplicate within each group
-- GROUP BY acts as a defensive layer to collapse any accidental duplicates from joins
,stacked_users_sponsors AS (
  SELECT
    educator_id AS user_id
    ,sponsor_id
    ,sponsor_name
    ,sponsor_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
  FROM attributions
  WHERE educator_id IS NOT NULL
  GROUP BY user_id, sponsor_id, sponsor_name, sponsor_code, classroom_id, classroom_name, classroom_code, site_id, site_name

  UNION ALL

  SELECT
    learner_id AS user_id
    ,sponsor_id
    ,sponsor_name
    ,sponsor_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
  FROM attributions
  WHERE learner_id IS NOT NULL
GROUP BY user_id, sponsor_id, sponsor_name, sponsor_code, classroom_id, classroom_name, classroom_code, site_id, site_name
)
-- Final output: one or more attribution records per user_id
SELECT
  user_id
  ,sponsor_id
  ,sponsor_name
  ,sponsor_code
  ,classroom_id
  ,classroom_name
  ,classroom_code
  ,site_id
  ,site_name
FROM stacked_users_sponsors