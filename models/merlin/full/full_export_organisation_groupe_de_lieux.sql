with export_organisation_groupe_de_lieux as (
    select * from  {{ ref('export_organisation_groupe_de_lieux') }}
),

final as (
    select 
    identifiant,
    nom,
    true actif
    from export_organisation_groupe_de_lieux
)

select * from final