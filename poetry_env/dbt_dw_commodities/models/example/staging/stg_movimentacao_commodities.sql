--import data from source

with source as (
    SELECT
        date,
        symbol,
        action,
        quantity
    from {{ source('commodities', 'movimentacao_commodities') }}
    ),

--renamed data

renamed as (
    SELECT
        cast("date" as date) as data,
        "symbol" as symbol,        
        action as acao,
        cast("quantity" as int) as quantidade
    FROM source
)

select * from renamed

-- select * from