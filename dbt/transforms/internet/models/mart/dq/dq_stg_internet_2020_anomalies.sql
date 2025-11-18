{{ config(materialized="view", schema="mart_grp2") }}

with cln as (
  select * from {{ source('clean_grp2','stg_internet_2020') }}
),
violations as (
  select
    id, region, name, province, municipality, internet,

    multiIf(
      id is null, 'null_code_id',
      region is null, 'null_code_region',
      name is null, 'null_code_name',
      province is null, 'null_code_province',
      municipality is null, 'null_code_municipality',
      internet not in ('1', '0'), 'invalid_internet',
      'ok'
    ) as dq_issue
  from cln
)
select *
from violations
where dq_issue != 'ok'
limit 50