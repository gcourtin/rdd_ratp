with export_ees_snapshot as (
    select * from  {{ ref('export_ees_snapshot') }}
),

final as (
    select
        id_sagai,
        libelle,
        sous_type,
        url_unc,
        case when dbt_valid_to is null then true else false end actif,
        commentaire,
        reseaux_fonctions,
        station,
        elo_id,
        x_peage
    from
        export_ees_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by id_sagai order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final