with source as (

    select * from {{ source('sagai_raw', 'utilisateur') }}

),

renamed as (

    select
        utilisateur_log,
        organisation_id,
        profil_id,
        utilisateur_pass,
        utilisateur_ess_conn,
        utilisateur_typ_emet,
        utilisateur_alr_stat,
        utilisateur_alr_del,
        utilisateur_nom,
        utilisateur_tel,
        utilisateur_inactif,
        utilisateur_droit,
        utilisateur_id_lieu,
        utilisateur_machine,
        utilisateur_org_id,
        utilisateur_pass_nochg,
        utilisateur_date_cnx,
        utilisateur_date_cre,
        utilisateur_utilisateur_cre,
        utilisateur_date_modif,
        utilisateur_utilisateur_modif,
        utilisateur_ema,
        utilisateur_com

    from source

)

select * from renamed