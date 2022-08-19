with source as (

    select * from {{ source('sagai_raw', 'zonegeo') }}

),

renamed as (

    select
        zonegeo_id,
        grlieu_id,
        collection_id,
        zonegeo_date_cre,
        zonegeo_utilisateur_cre,
        zonegeo_date_modif,
        zonegeo_utilisateur_modif

    from source

)

select * from renamed