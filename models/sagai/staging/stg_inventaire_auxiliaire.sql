with source as (

    select * from {{ source('sagai_raw', 'inventaire_auxiliaire') }}

),

renamed as (

    select
        inventaire_aux_id,
        inventaire_aux_libc,
        inventaire_a_date_cre,
        inventaire_a_utilisateur_cre,
        inventaire_a_date_modif,
        inventaire_a_utilisateur_modif

    from source

)

select * from renamed