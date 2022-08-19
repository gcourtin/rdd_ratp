with v_equipement_ratpi as (
    select * from {{ ref('net_mots_clefs') }}
),

v_ees as (
    select * from {{ ref('net_ees') }}
),

final as (
    select
        a.mot_clef as identifiant,
        trim(coalesce(a.num_ees,e.ees_libl))  as nom,
        true as actif,
        null as equipement_asservi,
        a.code_gmao as code_gmao,  
        null as responsable,
        a.elo_id as lieu,
        a.ees_id as ees,
        a.organisation_id as organisation,
        null as xpeage,
        'RATP-I' as source,
        a.mots_clefs_observations as description,  
        null as statut,
        a.regle_id as regle,
        a.sous_elo as code_mire
    from v_equipement_ratpi a
    inner join v_ees e on a.ees_id=e.ees_id    
    where (upper(a.mots_clefs_gmao) like '%GDI%' and upper(a.mot_clef) not like '%SAGAI%')
)

select * from final