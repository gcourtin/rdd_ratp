with source as (

    select * from {{ source('sagai_raw', 'fonction_ees') }}

),

renamed as (

    select
        ees_id,
        fonction_id,
        fonction_ees_date_cre,
        fonction_ees_utilisateur_cre,
        fonction_ees_date_modif,
        fonction_ees_utilisateur_modif

    from source

)

select * from renamed