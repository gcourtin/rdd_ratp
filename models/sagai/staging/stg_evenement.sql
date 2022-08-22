with source as (

    select * from {{ source('sagai_raw', 'evenement') }}

),

renamed as (

    select
        depeche_date,
        depeche_num,
        bloc_num,
        evt_num,
        evt_action,
        evt_date_cre,
        evt_heure_cre,
        evt_nom_enr,
        evt_org_enr,
        evt_nom_emet,
        evt_tel_emet,
        evt_date_cons,
        evt_heure_cons,
        evt_deg_urg,
        evt_delai,
        evt_gmao,
        evt_comment,
        evt_texte_csv,
        evt_url_doc1,
        evt_equipement_etat,
        reparation_id,
        evt_utilisateur_cre,
        evt_gmao_delai

    from source

)

select * from renamed