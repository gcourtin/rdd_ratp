with source as (

    select * from {{ source('sagai_raw', 'ees') }}

),

renamed as (

    select
        ees_id,
        ees_libl,
        ees_flag_ok_gene,
        ees_num_equip,
        ees_url,
        ees_dbl_depeche,
        ees_regroupement,
        ees_flag_maj,
        ees_date_cre,
        ees_utilisateur_cre,
        ees_date_modif,
        ees_utilisateur_modif

    from source

)

select * from renamed