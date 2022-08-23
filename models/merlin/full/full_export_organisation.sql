with export_organisation as (
    select * from  {{ ref('export_organisation') }}
),

final as (
    select
        identifiant,
        libelle,
        organisation_mere,
        societe_mere,
        type_d_organisation,
        tel_intervenant,
        actif,
        mode_de_propagation_par_defaut,
        mode_de_propagation_force,
        date_de_debut,
        date_de_fin,
        adresse_postale,
        mode_de_propagation_nominal,
        mode_de_propagation_nominal_cible,
        mode_de_propagation_degrade,
        mode_de_propagation_degrade_cible,
        commmentaire
    from
        export_organisation
)

select * from final