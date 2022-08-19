with source as (

    select * from {{ source('sagai_raw', 'regle_action') }}

),

renamed as (

    select
        regle_id,
        organisation_id,
        reg_act_typ_com,
        regle_action_date_cre,
        regle_action_utilisateur_cre,
        regle_action_date_modif,
        regle_action_utilisateur_modif

    from source

)

select * from renamed