with source as (

    select * from {{ source('sagai_raw', 'telephone') }}

),

renamed as (

    select
        elo_id,
        telephone

    from source

)

select * from renamed