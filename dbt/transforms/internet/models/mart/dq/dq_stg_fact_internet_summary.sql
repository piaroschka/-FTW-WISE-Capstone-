{{ config(materialized="view", schema="mart_grp2") }}


with src as (
  select reg, prv, mun, hsn, h16_a, h16_b, h16_c, h16_d from {{ source('raw_grp2','psa_census_2020_f3___all_regions') }}
),
cln as (
  select * from {{ source('clean_grp2','stg_fact_internet') }}
),

counts as (
  select
    (select count() from src)  as row_count_raw,
    (select count() from cln)  as row_count_clean
),
nulls as (
  select
    round(100.0 * countIf(reg is null) / nullif(count(),0), 2) as pct_null_reg,
    round(100.0 * countIf(prv is null) / nullif(count(),0), 2) as pct_null_mun,
    round(100.0 * countIf(mun is null) / nullif(count(),0), 2) as pct_null_prv,
    round(100.0 * countIf(hsn is null) / nullif(count(),0), 2) as pct_null_hsn
  from cln
),
domains as (
  select
    countIf(h16_a not in ('1', '2', '9')) as invalid_h16_a,
    countIf(h16_b not in ('1', '2', '9')) as invalid_h16_b,
    countIf(h16_c not in ('1', '2', '9')) as invalid_h16_c,
    countIf(h16_d not in ('1', '2', '9')) as invalid_h16_d
  from cln
),
joined as (
  select
    counts.row_count_raw,
    counts.row_count_clean,
    (counts.row_count_raw - counts.row_count_clean) as dropped_rows,

    nulls.pct_null_reg,
    nulls.pct_null_prv,
    nulls.pct_null_mun,
    nulls.pct_null_hsn,

    domains.invalid_h16_a,
    domains.invalid_h16_b,
    domains.invalid_h16_c,
    domains.invalid_h16_d

  from counts
  cross join nulls
  cross join domains
)
select * from joined