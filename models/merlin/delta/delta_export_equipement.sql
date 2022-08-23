with export_equipement_snapshot as (
    select * from  {{ ref('export_equipement_snapshot') }}
),

final as (
    select
        identifiant,
        nom,
        case when dbt_valid_to is null then true else false end actif,
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
        export_equipement_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by identifiant order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final