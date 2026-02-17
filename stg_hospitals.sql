/*
    STAGING MODEL: stg_hospitals
    =============================
    This is the first transformation layer. Staging models:
    - Rename columns to clean, consistent snake_case
    - Cast data types properly
    - Filter out bad/null records
    - Do NOT add business logic (that goes in marts)
    
    Think of staging as "make the raw data usable."
*/

with source as (
    -- Pull from the raw source we defined in sources.yml
    select * from {{ source('raw', 'hospitals') }}
),

cleaned as (
    select
        -- Rename columns to clean snake_case
        "Facility ID"           as facility_id,
        "Facility Name"         as facility_name,
        "Address"               as address,
        "City"                  as city,
        "State"                 as state,
        "ZIP Code"              as zip_code,
        "County Name"           as county_name,
        "Phone Number"          as phone_number,
        "Hospital Type"         as hospital_type,
        "Hospital Ownership"    as ownership_type,
        
        -- Cast rating to integer, handle 'Not Available'
        case
            when "Hospital overall rating" in ('Not Available', '')
                then null
            else cast("Hospital overall rating" as integer)
        end as overall_rating,
        
        -- Standardize boolean-like fields
        case 
            when "Emergency Services" = 'Yes' then true
            else false
        end as has_emergency_services,
        
        case 
            when "Meets criteria for promoting interoperability of EHRs" = 'Y' then true
            when "Meets criteria for promoting interoperability of EHRs" = 'N' then false
            else null
        end as meets_ehr_interop_criteria

    from source
    
    -- Filter out records with no facility ID (bad data)
    where "Facility ID" is not null
)

select * from cleaned
