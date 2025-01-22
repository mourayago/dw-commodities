--import data from source

with source as (
    SELECT
        "closeddate",
        "Close",        
        "symbol"
    FROM {{ source('commodities', 'commodities_data') }}
    ),

--renamed data

renamed as (
    SELECT
        cast("closeddate" as date) as "closeddate",
        "Close" as close_value,        
        "symbol"
    FROM source
)

select * from renamed

-- select * from