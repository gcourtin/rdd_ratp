with export_regle_de_routage_snapshot as (
    select * from  {{ ref('export_regle_de_routage_snapshot') }}
),

final as (
    select
        identifiant,
        libelle,
        description,
        case when dbt_valid_to is null then true else false end actif,
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
        export_regle_de_routage_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by identifiant order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final