with source as (

    select * from {{ source('sagai_raw', 'propagations') }}

),

renamed as (

    select
        propagation_id,
        propagation_action,
        propagation_degrade,
        propagation_typ_trans,
        propagation_version,
        propagation_date,
        propagation_login,
        propagation_date_cre,
        propagation_utilisateur_cre,
        propagation_date_modif,
        propagation_utilisateur_modif

    from source

)

select * from renamed