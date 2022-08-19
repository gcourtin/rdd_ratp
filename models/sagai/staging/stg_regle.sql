with source as (

    select * from {{ source('sagai_raw', 'regle') }}

),

renamed as (

    select
        regle_id,
        organisation_id,
        regle_libl,
        regle_dat_deb,
        regle_dat_fin,
        regle_typ_emet,
        regle_dat_arch,
        regle_typ_trans,
        regle_confirm,
        regle_opt_comment,
        regle_opt_arch,
        regle_dat_cre,
        regle_login_cre,
        regle_dat_der_modif,
        regle_login_der_modif,
        regle_desc,
        regle_poids,
        regle_date_cre,
        regle_utilisateur_cre,
        regle_date_modif,
        regle_utilisateur_modif

    from source

)

select * from renamed