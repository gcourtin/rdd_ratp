with v_net_mots_clefs as (
    select * from {{ ref('net_mots_clefs') }}
),

v_ees_xpeage as (
    select distinct ees_id from {{ ref('int_ees_xpeage') }} 
),

v_ees as (
    select * from {{ ref('net_ees') }}
),

final as (
    select
        a.mot_clef as identifiant,
        trim(coalesce(a.num_ees,e.ees_libl)) as nom,
        true as actif,
        null as equipement_asservi,
        a.code_gmao as code_gmao,  
        null as responsable,
        a.elo_id as lieu,
        a.ees_id as ees,
        a.organisation_id as organisation,
        null as xpeage,
        'AUTRES' as source,
        a.mots_clefs_observations as description,  
        null as statut,
        a.regle_id as regle,
        a.sous_elo as code_mire
    from v_net_mots_clefs a
    inner join v_ees e on a.ees_id=e.ees_id
    left join v_ees_xpeage b on a.ees_id=b.ees_id    
    where (upper(a.mot_clef) not like '%ARES%' and  upper(a.mot_clef)  not like '%SAGAI%')
    and (upper(a.mots_clefs_gmao) not like '%GDI%' and upper(a.mots_clefs_gmao) not like 'GMAO' and upper(a.mots_clefs_gmao) not like '%M2E%'
        or a.mots_clefs_gmao is null) 
    and a.ees_id is not null
    and a.elo_id is not null
    and b.ees_id is null   
)

select * from final