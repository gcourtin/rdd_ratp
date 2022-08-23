with export_organisation_fonction_snapshot as (
    select * from  {{ ref('export_organisation_fonction_snapshot') }}
),

final as (
    select
        identifiant,
        fonction_merlin,
        case when dbt_valid_to is null then true else false end actif
    from
        export_organisation_fonction_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by identifiant order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final