with export_organisation_snapshot as (
    select * from  {{ ref('export_organisation_snapshot') }}
),

final as (
    select
        identifiant,
        libelle,
        organisation_mere,
        societe_mere,
        type_d_organisation,
        tel_intervenant,
        case when dbt_valid_to is null then true else false end actif,
        mode_de_propagation_par_defaut,
        mode_de_propagation_force,
        date_de_debut,
        date_de_fin,
        adresse_postale,
        mode_de_propagation_nominal,
        mode_de_propagation_nominal_cible,
        mode_de_propagation_degrade,
        mode_de_propagation_degrade_cible,
        commmentaire
    from
        export_organisation_snapshot
    where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
    qualify dense_rank() over (partition by identifiant order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final