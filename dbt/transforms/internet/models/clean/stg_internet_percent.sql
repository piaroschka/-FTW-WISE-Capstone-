{{ config(materialized="table", schema="clean_grp2", tags=["staging"]) }}

SELECT
    region,
    province,
    municipality,
    has_internet,
    total_household,
    ROUND(100.0 * has_internet / total_household, 2) AS internet
FROM (
    SELECT
        id,
        region,
        province,
        municipality,
        COUNTIf(internet = 1) AS has_internet,
        COUNT(*) AS total_household
    FROM {{ source('clean_grp2', 'stg_internet_2020') }}
    GROUP BY id, region, province, municipality
) AS t
ORDER BY id, province, municipality