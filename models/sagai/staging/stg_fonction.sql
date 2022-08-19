with source as (

    select * from {{ source('sagai_raw', 'fonction') }}

),

renamed as (

    select
        fonction_id,
        fonction_libc,
        fonction_date_cre,
        fonction_utilisateur_cre,
        fonction_date_modif,
        fonction_utilisateur_modif

    from source

)

select * from renamed