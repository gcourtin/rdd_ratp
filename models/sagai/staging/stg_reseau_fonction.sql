with source as (

    select * from {{ source('sagai_raw', 'reseau_fonction') }}

),

renamed as (

    select
        reseau_id,
        fonction_id,
        res_fonc_date_cre,
        res_fonc_utilisateur_cre,
        res_fonc_date_modif,
        res_fonc_utilisateur_modif

    from source

)

select * from renamed