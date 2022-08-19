with source as (

    select * from {{ source('sagai_raw', 'regle_lieu') }}

),

renamed as (

    select
        regle_id,
        refpat112_id,
        regle_lieu_date_cre,
        regle_lieu_utilisateur_cre,
        regle_lieu_date_modif,
        regle_lieu_utilisateur_modif

    from source

)

select * from renamed