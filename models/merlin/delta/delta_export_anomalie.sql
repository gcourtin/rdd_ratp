with export_anomalie_snapshot as (
    select * from  {{ ref('export_anomalie_snapshot') }}
),

final as (
    select
        identifiant,
        libelle,
        description,
        type_d_equipement,
        case when dbt_valid_to is null then true else false end actif,
        niveau_d_urgence,
        etat_de_l_equipement,
        situation_inacceptable,
        critere_de_nettete,
        piece_jointe_obligatoire,
        verifications_exploitant,
        sensible
    from
        export_anomalie_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by identifiant order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final