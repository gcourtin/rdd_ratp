with v_equipement_agora as (
    select * from {{ ref('ext_equipement_agora') }}
),

v_net_ees as (
    select * from {{ ref('net_ees') }}
),

v_net_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_net_lieux as (
    select * from {{ ref('net_lieux') }}
),

final as (
    select
        a.code_bm as identifiant,
        coalesce(a.numero_ees,e.ees_libl) as nom,
        true as actif,
        null as equipement_asservi,
        a.code_bm as code_gmao,  
        null as responsable,
        a.code_refpat as lieu,
        a.ees_id as ees,
        replace(a.service_maint,'-','/') as organisation,
        a.x_peage as xpeage,
        'AGORA' as source,
        a.description_bm as description,  
        a.statut as statut,
        null as regle,
        case when length(a.code_refpat)=17 then right(a.code_refpat,5) else null end as code_mire
    from v_equipement_agora a
    inner join v_net_ees e on e.ees_id=a.ees_id
    inner join v_net_organisation o on o.organisation_id = replace(a.service_maint,'-','/')
    inner join v_net_lieux l on l.elo_id=a.code_refpat
    -- where (a.ees_id is null or e.ees_id is not null)
    -- and   (replace(a.service_maint,'-','/') is null or o.organisation_id is not null)
    -- and   (a.code_refpat is null or l.elo_id is not null)
)

select * from final