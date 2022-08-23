with export_equipement as (
    select * from  {{ ref('export_equipement') }}
),

final as (
    select
        identifiant,
        nom,
       actif,
        equipement_asservi,
        code_gmao,
        responsable,
        lieu,
        ees,
        organisation,
        xpeage,
        source,
        description,
        statut,
        regle,
        code_mire
    from
        export_equipement
)

select * from final