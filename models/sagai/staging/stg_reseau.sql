with source as (

    select * from {{ source('sagai_raw', 'reseau') }}

),

renamed as (

    select
        reseau_id,
        reseau_libc,
        reseau_date_cre,
        reseau_utilisateur_cre,
        reseau_date_modif,
        reseau_utilisateur_modif

    from source

)

select * from renamed