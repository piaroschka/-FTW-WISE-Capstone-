{{ config(materialized="view", schema="mart_grp2") }}

with cln as (
  select * from {{ source('clean_grp2','stg_2019_social_media_penetration') }}
),
violations as (
  select
    id, region, name, total_socmed_users, total_regional_repondents, percent_socmed_users,

    multiIf(
      id is null, 'null_id',
      region is null, 'null_region',
      name is null, 'null_name',
      total_socmed_users is null, 'null_total_socmed_users',
      total_regional_repondents is null, 'null_total_regional_respondents',
      percent_socmed_users is null, 'null_percent_socmed_users',
      'ok'
    ) as dq_issue
  from cln
)
select *
from violations
where dq_issue != 'ok'
limit 50