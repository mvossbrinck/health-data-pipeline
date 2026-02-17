/*
    MART MODEL: mart_hospital_quality
    ==================================
    Hospital-level detail enriched with quality flags and classifications.
    This is the kind of table you'd use to power a dashboard or feed into
    a machine learning model (e.g., predicting which hospitals might drop
    in rating next quarter).
*/

with hospitals as (
    select * from {{ ref('stg_hospitals') }}
),

enriched as (
    select
        facility_id,
        facility_name,
        city,
        state,
        zip_code,
        county_name,
        hospital_type,
        ownership_type,
        overall_rating,
        has_emergency_services,
        meets_ehr_interop_criteria,
        
        -- Quality classification
        case
            when overall_rating >= 4 then 'High Quality'
            when overall_rating = 3 then 'Average'
            when overall_rating <= 2 then 'Needs Improvement'
            else 'Not Rated'
        end as quality_classification,
        
        -- Ownership simplified
        case
            when ownership_type like '%non-profit%' then 'Non-Profit'
            when ownership_type = 'Proprietary' then 'For-Profit'
            when ownership_type like '%Government%' then 'Government'
            else 'Other'
        end as ownership_category,
        
        -- Access risk flag: low-rated + no emergency services
        case
            when overall_rating <= 2 and not has_emergency_services
                then true
            else false
        end as access_risk_flag

    from hospitals
)

select * from enriched
