{{ config(materialized="view", schema="mart_grp2") }}


with src as (
  select reg, prv, mun, hsn, h16_a, h16_b, h16_c, h16_d from {{ source('raw_grp2','social_media_penetration') }}
),
cln as (
  select * from {{ source('clean_grp2','stg_2019_social_media_penetration') }}
),

counts as (
  select
    (select count() from src)  as row_count_raw,
    (select count() from cln)  as row_count_clean
),
nulls as (
  select
    round(100.0 * countIf(id is null) / nullif(count(),0), 2) as pct_null_id,
    round(100.0 * countIf(region is null) / nullif(count(),0), 2) as pct_null_region,
    round(100.0 * countIf(name is null) / nullif(count(),0), 2) as pct_null_name,
    round(100.0 * countIf(total_socmed_users is null) / nullif(count(),0), 2) as pct_null_total_socmed_users,
    round(100.0 * countIf(total_regional_repondents is null) / nullif(count(),0), 2) as pct_null_total_regional_repondents,
    round(100.0 * countIf(percent_socmed_users is null) / nullif(count(),0), 2) as pct_null_percent_socmed_users
  from cln
),
joined as (
  select
    counts.row_count_raw,
    counts.row_count_clean,
    (counts.row_count_raw - counts.row_count_clean) as dropped_rows,

    nulls.pct_null_id,
    nulls.pct_null_region,
    nulls.pct_null_name,
    nulls.pct_null_total_socmed_users,
    nulls.pct_null_total_regional_repondents,
    nulls.pct_null_percent_socmed_users,

  from counts
  cross join nulls
)
select * from joined