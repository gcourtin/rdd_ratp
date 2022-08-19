with source as (

    select * from {{ source('sagai_raw', 'ees_generique') }}

),

renamed as (

    select
        grlieu_id,
        ees_id,
        ees_gen_date_cre,
        ees_gen_utilisateur_cre,
        ees_gen_date_modif,
        ees_gen_utilisateur_modif

    from source

)

select * from renamed