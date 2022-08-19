with source as (

    select * from {{ source('sagai_raw', 'organisation') }}

),

renamed as (

    select
        organisation_id,
        organisation_id_pere,
        organisation_nom,
        organisation_type,
        organisation_code_refpat,
        organisation_typ_trans,
        organisation_typ_deg,
        organisation_tel,
        organisation_fax,
        organisation_imp,
        organisation_typ_ema,
        organisation_ema,
        organisation_ema_sujet,
        organisation_adr,
        propagation_id,
        organisation_num_etat,
        organisation_autopropa,
        organisation_nftf_auto,
        organisation_lieu_id,
        organisation_sagai2,
        zonegeo_id,
        grlieu_id,
        organisation_impr_auto,
        organisation_contacts,
        organisation_dbl_emis,
        organisation_dbl_depeche,
        fonction_id,
        organisation_ema_nbmax,
        organisation_date_cre,
        organisation_utilisateur_cre,
        organisation_date_modif,
        organisation_utilisateur_modif,
        organisation_ano_visi,
        organisation_com

    from source
)

select * from renamed