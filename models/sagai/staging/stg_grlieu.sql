with source as (

    select * from {{ source('sagai_raw', 'grlieu') }}

),

renamed as (

    select
        grlieu_id,
        grlieu_reseau_elo_id,
        elo_id

    from source

)

select * from renamed