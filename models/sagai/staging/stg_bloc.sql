with source as (

    select * from {{ source('sagai_raw', 'bloc') }}

),

renamed as (

    select
        depeche_date,
        depeche_num,
        bloc_num,
        bloc_num_pere,
        bloc_statut,
        bloc_action,
        bloc_date_cre,
        bloc_heure_cre,
        bloc_regle_id,
        bloc_nom_enr,
        bloc_org_enr,
        bloc_asi_id,
        bloc_nom_emet,
        bloc_tel_emet,
        bloc_date_cons,
        bloc_heure_cons,
        bloc_deg_urg,
        bloc_lieu_id,
        bloc_desc_lieu,
        bloc_code_mire,
        bloc_localis,
        bloc_type_voie,
        bloc_voie,
        bloc_file,
        bloc_pk_deb,
        bloc_pk_fin,
        bloc_aile,
        bloc_etage,
        bloc_bureau_local,
        bloc_com_lieu,
        bloc_ees_id,
        bloc_desc_equip,
        bloc_num_equip,
        bloc_ano_id,
        bloc_desc_ano,
        bloc_org_id,
        bloc_equip_avise,
        bloc_delai,
        bloc_gmao,
        bloc_comment,
        bloc_commentaire_exploitant,
        bloc_reseau_elo_id,
        bloc_texte_csv,
        bloc_url_doc1,
        bloc_equipement_etat,
        mot_clef,
        reparation_id,
        bloc_utilisateur_cre,
        bloc_im_piece_j

    from source

)

select * from renamed