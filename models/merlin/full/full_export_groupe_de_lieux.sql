with export_groupe_de_lieux as (
    select * from  {{ ref('export_groupe_de_lieux') }}
),

final as (
    select
        identifiant,
        type_espace,
        parent,
        elo_id,
        actif,
        description
    from
        export_groupe_de_lieux
)

select * from final