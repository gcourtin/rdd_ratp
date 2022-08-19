with source as (

    select * from {{ source('sagai_raw', 'inter_station') }}

),

renamed as (

    select
        station_elo_id,
        inter_elo_id,
        is_date_cre,
        is_utilisateur_cre,
        is_date_modif,
        is_utilisateur_modif

    from source

)

select * from renamed