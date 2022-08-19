with source as (

    select * from {{ source('sagai_raw', 'adresse_refpat') }}

),

renamed as (

    select
        adresse_elo_id,
        adresse_adr

    from source

)

select * from renamed