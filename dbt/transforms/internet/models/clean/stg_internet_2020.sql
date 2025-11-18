{{ config(materialized="table", schema="clean_grp2", tags=["staging"]) }}

WITH base as (
select
  cast(reg      as Int64)     as reg,
  cast(prv      as Nullable(Int64))     as prov,
  cast(mun      as Nullable(Int64))     as mun,
  cast(hsn      as Int64)     as hsn,
  cast(h16_a    as Int64)     as h16_a,
  cast(h16_b    as Int64)     as h16_b,
  cast(h16_c    as Int64)     as h16_c,
  cast(h16_d    as Int64)     as h16_d,
  cast( case
            when h16_a = 1 OR h16_b = 1 OR h16_c = 1 OR h16_d = 1 then 1
            else 0
        end as Int64) as internet
from {{ source('raw_grp2', 'psa_census_2020_f3___all_regions') }}),

geo as (
    select
        reg,
        prov,
        mun,
        any(name) as municipality_name
    from {{ source('clean_grp2', 'stg_dim_geography') }}
    where brgy IS NULL
    group by reg, prov, mun

),

province as (
    select
        reg,
        prov,
        any(name) as province_name
    from {{ source('clean_grp2', 'stg_dim_geography') }}
    where brgy IS NULL and mun IS NULL
    group by reg, prov
),

region as (
    select
        id,
        any(region) as region_code,
        any(name) as region_name
    from {{ source('clean_grp2', 'stg_dim_region') }}
    group by id
)

select
    b.reg as id,
    r.region_code as region,
    r.region_name as name,
    p.province_name as province,
    g.municipality_name as municipality,
    b.internet as internet
from base b
left join geo g
    on b.reg = g.reg
   and b.prov = g.prov
   and b.mun = g.mun
left join province p
    on b.reg = p.reg
   and b.prov = p.prov
left join region r
    on b.reg = r.id
