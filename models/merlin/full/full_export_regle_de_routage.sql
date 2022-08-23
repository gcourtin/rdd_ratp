with export_regle_de_routage as (
    select * from  {{ ref('export_regle_de_routage') }}
),

final as (
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
    from
        export_regle_de_routage
)

select * from final