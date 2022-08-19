with source as (

    select * from {{ source('sagai_raw', 'regle_ees') }}

),

renamed as (

    select
        regle_id,
        ees_id,
        regle_ees_date_cre,
        regle_ees_utilisateur_cre,
        regle_ees_date_modif,
        regle_ees_utilisateur_modif

    from source

)

select * from renamed