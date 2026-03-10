-- models/intermediate/int_user_attributions.sql
--
-- Intermediate model: Unifies multiple user attribution paths into one grain
-- Captures four distinct onboarding/join routes: classroom membership (learner/educator),
-- classroom invitation (email match), and direct sponsor invite code join.
-- Output can have multiple rows per user_id (one per attribution path).
-- Downstream dim_users.sql deduplicates and selects canonical values.

with attributions as (
  -- Route 1: Learners via classroom → site → sponsor
  select
    null as educator_id
    ,learner_classroom_membership.user_id as learner_id
    ,classroom.site_id
    ,user_site.name as site_name
    ,user_sponsor.id as sponsor_id
    ,user_sponsor.name as sponsor_name
    ,sponsor_invite_code.code as sponsor_code
    ,classroom.id as classroom_id
    ,classroom.name as classroom_name
    ,classroom_invite_code.code as classroom_code
  from {{ source('raw','learner_classroom_membership') }}
  left join {{ source('raw','classroom') }} on classroom.id = learner_classroom_membership.classroom_id
  left join {{ source('raw','user_site') }} on classroom.site_id = user_site.id
  left join {{ source('raw','user_sponsor') }} on user_site.sponsor_id = user_sponsor.id
  left join {{ source('raw','sponsor_invite_code') }} on sponsor_invite_code.sponsor_id = user_sponsor.id
  left join {{ source('raw','classroom_invite_code') }} on classroom_invite_code.classroom_id = classroom.id

  union all

  -- Route 2: Educators via classroom → site → sponsor
  select
    educator_classroom_membership.user_id as educator_id
    ,null as learner_id
    ,classroom.site_id
    ,user_site.name as site_name
    ,user_sponsor.id as sponsor_id
    ,user_sponsor.name as sponsor_name
    ,sponsor_invite_code.code as sponsor_code
    ,classroom.id as classroom_id
    ,classroom.name as classroom_name
    ,classroom_invite_code.code as classroom_code
  from {{ source('raw','educator_classroom_membership') }}
  left join {{ source('raw','classroom') }} on educator_classroom_membership.classroom_id = classroom.id
  left join {{ source('raw','user_site') }} on classroom.site_id = user_site.id
  left join {{ source('raw','user_sponsor') }} on user_site.sponsor_id = user_sponsor.id
  left join {{ source('raw','sponsor_invite_code') }} on sponsor_invite_code.sponsor_id = user_sponsor.id
  left join {{ source('raw','classroom_invite_code') }} on classroom_invite_code.classroom_id = classroom.id

  union all

  -- Route 3: Learners invited via classroom invitation (matched by email)
  select
    null as educator_id
    ,user_core.id as learner_id
    ,classroom.site_id
    ,user_site.name as site_name
    ,user_sponsor.id as sponsor_id
    ,user_sponsor.name as sponsor_name
    ,sponsor_invite_code.code as sponsor_code
    ,classroom.id as classroom_id
    ,classroom.name as classroom_name
    ,classroom_invite_code.code as classroom_code
  from {{ source('raw','educator_invitation_record') }}
  join {{ source('raw','user_core') }} 
    on lower(trim(user_core.email)) = lower(trim(educator_invitation_record.email)) 
    and user_core.type != 'IL' -- Exclude Independent Learners as they don't use invitations
  join {{ source('raw','classroom') }} on classroom.id = educator_invitation_record.classroom_id
  left join {{ source('raw','user_site') }} on classroom.site_id = user_site.id
  left join {{ source('raw','user_sponsor') }} on user_site.sponsor_id = user_sponsor.id
  left join {{ source('raw','sponsor_invite_code') }} on sponsor_invite_code.sponsor_id = user_sponsor.id
  left join {{ source('raw','classroom_invite_code') }} on classroom_invite_code.classroom_id = classroom.id

  union all

  -- Route 4: Learners who joined via sponsor invite code
  select
    null as educator_id
    ,user_core.id as learner_id
    ,sponsor_invite_code.site_id
    ,user_site.name as site_name
    ,user_sponsor.id as sponsor_id
    ,user_sponsor.name as sponsor_name
    ,sponsor_invite_code.code as sponsor_code
    ,null as classroom_id
    ,null as classroom_name
    ,null as classroom_code
  from {{ source('raw','user_join_record') }}
  join {{ source('raw','user_core') }} 
    on user_core.id = user_join_record.user_id 
    and user_core.type != 'IL' -- Exclude Independent Learners
  join {{ source('raw','sponsor_invite_code') }} on user_join_record.sponsor_invite_code_id = sponsor_invite_code.id
  left join {{ source('raw','user_sponsor') }} on sponsor_invite_code.sponsor_id = user_sponsor.id
  left join {{ source('raw','user_site') }} on sponsor_invite_code.site_id = user_site.id
  where user_join_record.action_type = 'userjoins'
)
-- Stack educator and learner attributions, deduplicate within each group
-- GROUP BY acts as a defensive layer to collapse any accidental duplicates from joins
,stacked_users_sponsors as (
  select
    educator_id as user_id
    ,sponsor_id
    ,sponsor_name
    ,sponsor_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
  from attributions
  where educator_id is not null
  group by user_id, sponsor_id, sponsor_name, sponsor_code, classroom_id, classroom_name, classroom_code, site_id, site_name

  union all

  select
    learner_id as user_id
    ,sponsor_id
    ,sponsor_name
    ,sponsor_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
  from attributions
  where learner_id is not null
group by user_id, sponsor_id, sponsor_name, sponsor_code, classroom_id, classroom_name, classroom_code, site_id, site_name
)
-- Final output: one or more attribution records per user_id
select 
    user_id
    ,sponsor_id
    ,sponsor_name
    ,sponsor_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
from stacked_users_sponsors