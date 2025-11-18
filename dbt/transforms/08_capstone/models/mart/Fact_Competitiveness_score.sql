{{ config(materialized="view") }}

select
  stg_competitiveness_score.competitiveness_score,
  stg_competitiveness_score.city,
  stg_competitiveness_score.province,
  stg_competitiveness_score.region,
  dr.reg,                    -- From Dim_region
  vt.prv,                    -- From Voter_turnout_2022  
  vt.city as voter_city,     -- From Voter_turnout_2022
  -- Calculate poverty percentage
  case 
    when stg_competitiveness_score.competitiveness_score is not null 
         and stg_competitiveness_score.competitiveness_score != 100
    then 100 - stg_competitiveness_score.competitiveness_score 
    else null 
  end as percent_poverty
from {{ ref('stg_competitiveness_score') }}
left join {{ ref('Dim_region') }} dr
  on stg_competitiveness_score.region = dr.region
left join {{ ref('Voter_turnout_2022') }} vt
  on stg_competitiveness_score.province = vt.prv 
  and stg_competitiveness_score.city = vt.city