{{ config(materialized="view", schema="mart_grp2") }}


with src as (
  select * from {{ source('raw_grp2','psa_census_2020_f3___all_regions') }}
),
cln as (
  select * from {{ source('clean_grp2','stg_internet_2020') }}
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
    round(100.0 * countIf(province is null) / nullif(count(),0), 2) as pct_null_province,
    round(100.0 * countIf(municipality is null) / nullif(count(),0), 2) as pct_null_municipality
  from cln
),
domains as (
  select
    countIf(internet not in ('1', '0')) as invalid_internet
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
    nulls.pct_null_province,
    nulls.pct_null_municipality,

    domains.invalid_internet

  from counts
  cross join nulls
  cross join domains
)
select * from joined