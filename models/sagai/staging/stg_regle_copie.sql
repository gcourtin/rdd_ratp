with source as (

    select * from {{ source('sagai_raw', 'regle_copie') }}

),

renamed as (

    select
        regle_id,
        organisation_id,
        reg_cop_typ_com,
        regle_copie_date_cre,
        regle_copie_utilisateur_cre,
        regle_copie_date_modif,
        regle_copie_utilisateur_modif

    from source

)

select * from renamed