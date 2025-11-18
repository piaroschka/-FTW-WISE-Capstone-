{{ config(materialized="view", schema="mart_grp2") }}

with cln as (
  select * from {{ source('clean_grp2','stg_fact_internet') }}
),
violations as (
  select
    reg, prv, mun, hsn, h16_a, h16_b, h16_c, h16_d,

    multiIf(
      reg is null, 'null_code_reg',
      prv is null, 'null_code_prv',
      mun is null, 'null_code_mun',
      hsn is null, 'null_code_hsn',
      h16_a not in ('1', '2', '9'), 'invalid_h16_a',
      h16_b not in ('1', '2', '9'), 'invalid_h16_b',
      h16_c not in ('1', '2', '9'), 'invalid_h16_c',
      h16_d not in ('1', '2', '9'), 'invalid_h16_d',
      'ok'
    ) as dq_issue
  from cln
)
select *
from violations
where dq_issue != 'ok'
limit 50