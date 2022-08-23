with export_organisation_fonction as (
    select * from  {{ ref('export_organisation_fonction') }}
),

final as (
    select
        identifiant,
        fonction_merlin,
        true actif
    from
        export_organisation_fonction
)

select * from final