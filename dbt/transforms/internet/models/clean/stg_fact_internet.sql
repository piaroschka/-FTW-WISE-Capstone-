{{ config(materialized="table", schema="clean_grp2", tags=["staging"]) }}

select
  cast(reg      as Int64)     as reg,
  cast(prv      as Int64)     as prv,
  cast(mun      as Int64)     as mun,
  cast(hsn      as Int64)     as hsn,
  cast(h16_a    as Int64)     as h16_a,
  cast(h16_b    as Int64)     as h16_b,
  cast(h16_c    as Int64)     as h16_c,
  cast(h16_d    as Int64)     as h16_d
from {{ source('raw_grp2', 'psa_census_2020_f3___all_regions') }}