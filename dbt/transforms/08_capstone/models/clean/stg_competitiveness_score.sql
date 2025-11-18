-- models/clean/stg_competitiveness_score.sql
{{ config(materialized="view") }}

with first_second_class as (
  select
    'first_second_class' as city_type,
    -- Column 2: competitiveness_score
    toFloat64OrZero(column2) as competitiveness_score,
    -- Column 3: city
    trim(ifNull(column3, '')) as city,
    -- Column 4: province
    trim(ifNull(column4, '')) as province,
    -- Column 5: region - remove parentheses and content inside
    trim(
      case 
        when contains(column5, '(') 
        then substring(column5, 1, position('(' in column5) - 1)
        else ifNull(column5, '')
      end
    ) as region
  from {{ source('raw', 'first_second_class_municipalities') }}
  where column3 is not null and column3 != ''
),

third_fourth_class as (
  select
    'third_fourth_class' as city_type,
    toFloat64OrZero(column2) as competitiveness_score,
    trim(ifNull(column3, '')) as city,
    trim(ifNull(column4, '')) as province,
    trim(
      case 
        when contains(column5, '(') 
        then substring(column5, 1, position('(' in column5) - 1)
        else ifNull(column5, '')
      end
    ) as region
  from {{ source('raw', 'third_fourth_class_municipalities') }}
  where column3 is not null and column3 != ''
),

component_cities as (
  select
    'component_cities' as city_type,
    toFloat64OrZero(column2) as competitiveness_score,
    trim(ifNull(column3, '')) as city,
    trim(ifNull(column4, '')) as province,
    trim(
      case 
        when contains(column5, '(') 
        then substring(column5, 1, position('(' in column5) - 1)
        else ifNull(column5, '')
      end
    ) as region
  from {{ source('raw', 'component_cities') }}
  where column3 is not null and column3 != ''
),

highly_urbanized as (
  select
    'highly_urbanized' as city_type,
    toFloat64OrZero(column2) as competitiveness_score,
    trim(ifNull(column3, '')) as city,
    trim(ifNull(column4, '')) as province,
    trim(
      case 
        when contains(column5, '(') 
        then substring(column5, 1, position('(' in column5) - 1)
        else ifNull(column5, '')
      end
    ) as region
  from {{ source('raw', 'highly_urbanized_cities') }}
  where column3 is not null and column3 != ''
),

-- Combine all cities using UNION ALL
all_cities as (
  select * from first_second_class
  union all
  select * from third_fourth_class
  union all
  select * from component_cities
  union all
  select * from highly_urbanized
)

-- Final selection with cleaned columns
select
  city_type,
  competitiveness_score,
  city,
  province,
  region
from all_cities