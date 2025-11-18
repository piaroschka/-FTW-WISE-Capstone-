{{ config(
    materialized='table',
    schema='clean_grp2',
    tags=['staging', 'social_media_penetration']
) }}

SELECT
    id,
    region,
    name,
    CAST(total_internet_users AS Int32) AS total_socmed_users,
    CAST(total_regional_respondents AS Int32) AS total_regional_respondents,
    ROUND(
        CAST(
            CASE
                WHEN percent_users < 0 THEN 0
                WHEN percent_users > 1 THEN 1
                ELSE percent_users
            END * 100 AS Float32
        ),
        2
    ) AS percent_socmed_users

FROM {{ source('raw', 'social_media_penetration') }}
