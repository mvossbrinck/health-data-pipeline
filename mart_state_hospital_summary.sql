/*
    MART MODEL: mart_state_hospital_summary
    =========================================
    This is the business-logic layer. Marts are what analysts and dashboards
    actually query. This model answers: "What does hospital quality and access
    look like at the state level?"
    
    This is where you add:
    - Aggregations
    - Business calculations
    - Metrics stakeholders care about
    
    In a company like Zivian (provider compliance) or PatientPoint (point-of-care),
    this kind of geographic rollup helps answer questions like:
    - "Which states have the most hospitals but lowest ratings?"
    - "Where are the gaps in emergency services coverage?"
    - "What % of hospitals in each state meet EHR interoperability criteria?"
*/

with hospitals as (
    -- Pull from the staging model (not raw!)
    -- dbt knows the dependency chain: raw → staging → mart
    select * from {{ ref('stg_hospitals') }}
),

state_summary as (
    select
        state,
        
        -- Volume metrics
        count(*) as total_hospitals,
        count(case when hospital_type = 'Acute Care Hospitals' then 1 end) as acute_care_count,
        count(case when hospital_type = 'Critical Access Hospitals' then 1 end) as critical_access_count,
        
        -- Quality metrics
        round(avg(overall_rating), 2) as avg_rating,
        count(case when overall_rating >= 4 then 1 end) as high_rated_count,
        count(case when overall_rating <= 2 then 1 end) as low_rated_count,
        
        -- Access metrics
        count(case when has_emergency_services then 1 end) as emergency_services_count,
        round(
            100.0 * count(case when has_emergency_services then 1 end) / count(*), 
            1
        ) as pct_with_emergency,
        
        -- Technology adoption
        round(
            100.0 * count(case when meets_ehr_interop_criteria then 1 end) 
            / nullif(count(case when meets_ehr_interop_criteria is not null then 1 end), 0),
            1
        ) as pct_ehr_interop,
        
        -- Ownership breakdown
        count(case when ownership_type like '%non-profit%' then 1 end) as nonprofit_count,
        count(case when ownership_type = 'Proprietary' then 1 end) as for_profit_count,
        count(case when ownership_type like '%Government%' then 1 end) as government_count

    from hospitals
    group by state
)

select
    *,
    -- Derived metrics
    round(100.0 * high_rated_count / total_hospitals, 1) as pct_high_rated,
    round(100.0 * nonprofit_count / total_hospitals, 1) as pct_nonprofit,
    
    -- Simple classification
    case
        when avg_rating >= 3.5 then 'Above Average'
        when avg_rating >= 2.5 then 'Average'
        else 'Below Average'
    end as quality_tier

from state_summary
order by total_hospitals desc
