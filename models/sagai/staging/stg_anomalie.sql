with source as (

    select * from {{ source('sagai_raw', 'anomalie') }}

),

renamed as (

    select
        anomalie_id,
        ees_id,
        anomalie_libl,
        anomalie_no_rang,
        anomalie_niv_urg,
        anomalie_inacceptable,
        anomalie_niv_urg_mes,
        anomalie_nettete,
        anomalie_es_hs,
        anomalie_delai,
        anomalie_forfait,
        anomalie_date_cre,
        anomalie_utilisateur_cre,
        anomalie_date_modif,
        anomalie_utilisateur_modif

    from source

)

select * from renamed