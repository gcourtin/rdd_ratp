with source as (

    select * from {{ source('sagai_raw', 'mots_clefs') }}

),

renamed as (

    select
        mot_clef,
        refpat112_id,
        ees_id,
        num_equip,
        elo_id,
        regle_id,
        organisation_id,
        mots_clefs_nombre,
        num_ees,
        code_gmao,
        mots_clefs_flag_equip_sensible,
        mots_clefs_observations,
        mots_clefs_date_modif,
        mots_clefs_gmao,
        inventaire_aux_id,
        periode_sensible_id,
        mots_clefs_pk_deb,
        mots_clefs_date_cre,
        mots_clefs_utilisateur_cre,
        mots_clefs_utilisateur_modif,
        timestamp_tz_maj_org,
        nature,
        telephone,
        fonction_locale,
        sous_elo,
        sous_elo_num_ligne

    from source

)

select * from renamed