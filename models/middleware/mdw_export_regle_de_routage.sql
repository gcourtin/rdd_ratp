with source as (

    select * from {{ source('mdw_export', 'export_regle_de_routage') }}

),

renamed as (

    select
        identifiant,
        libelle,
        description,
        actif,
        date_debut,
        date_fin,
        plage_horaire,
        heure_de_debut,
        heure_de_fin,
        poids,
        lieux,
        ees,
        equipement,
        anomalie,
        intervenant_principal,
        intervenant_pour_action,
        intervenant_en_copie

    from source

)

select * from renamed