with source as (

    select * from {{ source('sagai_raw', 'regle_ano') }}

),

renamed as (

    select
        regle_id,
        anomalie_id,
        regle_ano_date_cre,
        regle_ano_utilisateur_cre,
        regle_ano_date_modif,
        regle_ano_utilisateur_modif

    from source

)

select * from renamed