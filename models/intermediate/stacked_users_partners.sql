with attributions as (
  -- Route 1: Learners via classroom → site → partner
  select
    null as educator_id
    ,educator_classroomlearnermembership.user_id as learner_id
    ,educator_classroom.site_id
    ,user_site.name as site_name
    ,user_partner.id as partner_id
    ,user_partner.name as partner_name
    ,user_partnerinvitecode.code as partner_code
    ,educator_classroom.id as classroom_id
    ,educator_classroom.name as classroom_name,
    ,educator_classroominvitecode.code as classroom_code
  from {{ source('raw','educator_classroomlearnermembership') }}
  left join {{ source('raw','educator_classroom') }} on educator_classroom.id = educator_classroomlearnermembership.classroom_id
  left join {{ source('raw','user_site') }} on educator_classroom.site_id = user_site.id
  left join {{ source('raw','user_partner') }} on user_site.partner_id = user_partner.id
  left join {{ source('raw','user_partnerinvitecode') }} on user_partnerinvitecode.partner_id = user_partner.id
  left join {{ source('raw','educator_classroominvitecode') }} on educator_classroominvitecode.classroom_id = educator_classroom.id

  union all

  -- Route 2: Educators via classroom → site → partner
  select
    educator_classroom_educators.user_id as educator_id
    ,null as learner_id
    ,educator_classroom.site_id
    ,user_site.name as site_name
    ,user_partner.id as partner_id
    ,user_partner.name as partner_name
    ,user_partnerinvitecode.code as partner_code
    ,educator_classroom.id as classroom_id
    ,educator_classroom.name as classroom_name
    ,educator_classroominvitecode.code as classroom_code
  from {{ source('raw','educator_classroom_educators') }}
  left join {{ source('raw','educator_classroom') }} on educator_classroom_educators.classroom_id = educator_classroom.id
  left join {{ source('raw','user_site') }} on educator_classroom.site_id = user_site.id
  left join {{ source('raw','user_partner') }} on user_site.partner_id = user_partner.id
  left join {{ source('raw','user_partnerinvitecode') }} on user_partnerinvitecode.partner_id = user_partner.id
  left join {{ source('raw','educator_classroominvitecode') }} on educator_classroominvitecode.classroom_id = educator_classroom.id

  union all

  -- Route 3: Learners invited via classroom invitation (matched by email)
  select
    null as educator_id
    ,user_user.id as learner_id
    ,educator_classroom.site_id
    ,user_site.name as site_name
    ,user_partner.id as partner_id
    ,user_partner.name as partner_name
    ,user_partnerinvitecode.code as partner_code
    ,educator_classroom.id as classroom_id
    ,educator_classroom.name as classroom_name
    ,educator_classroominvitecode.code as classroom_code
  from {{ source('raw','educator_classroominvitation') }}
  join {{ source('raw','user_user') }} on lower(trim(user_user.email)) = lower(trim(educator_classroominvitation.email)) and user_user.type != 'IL'
  join {{ source('raw','educator_classroom') }} on educator_classroom.id = educator_classroominvitation.classroom_id
  left join {{ source('raw','user_site') }} on educator_classroom.site_id = user_site.id
  left join {{ source('raw','user_partner') }} on user_site.partner_id = user_partner.id
  left join {{ source('raw','user_partnerinvitecode') }} on user_partnerinvitecode.partner_id = user_partner.id
  left join {{ source('raw','educator_classroominvitecode') }} on educator_classroominvitecode.classroom_id = educator_classroom.id

  union all

  -- Route 4: Learners who joined via partner invite code
  select
    null as educator_id
    ,user_user.id as learner_id
    ,user_partnerinvitecode.site_id
    ,user_site.name as site_name
    ,user_partner.id as partner_id
    ,user_partner.name as partner_name
    ,user_partnerinvitecode.code as partner_code
    ,null as classroom_id
    ,null as classroom_name
    ,null as classroom_code
  from {{ source('raw','action_userjoinsaction') }}
  join {{ source('raw','user_user') }} on user_user.id = action_userjoinsaction.user_id and user_user.type != 'IL'
  join {{ source('raw','user_partnerinvitecode') }} as user_partnerinvitecode on action_userjoinsaction.partner_invite_code_id = user_partnerinvitecode.id
  left join {{ source('raw','user_partner') }} on user_partner.id = user_partnerinvitecode.partner_id
  left join {{ source('raw','user_site') }} on user_partnerinvitecode.site_id = user_site.id
  where action_userjoinsaction.action_type = 'userjoins'
)
, stacked_users_partners as (
  select
    educator_id as user_id
    ,partner_id
    ,partner_name
    ,partner_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
  from attributions
  where educator_id is not null
  group by 1,2,3,4,5,6,7,8,9

  union all

  select
    learner_id as user_id
    ,partner_id
    ,partner_name
    ,partner_code
    ,classroom_id
    ,classroom_name
    ,classroom_code
    ,site_id
    ,site_name
  from attributions
  where learner_id is not null
  group by 1,2,3,4,5,6,7,8,9
)

select *
from stacked_users_partners
