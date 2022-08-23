with export_groupe_de_lieux_snapshot as (
    select * from  {{ ref('export_groupe_de_lieux_snapshot') }}
),

final as (
    select
        identifiant,
        type_espace,
        parent,
        elo_id,
        case when dbt_valid_to is null then true else false end actif,
        description
    from
        export_groupe_de_lieux_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by identifiant order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final