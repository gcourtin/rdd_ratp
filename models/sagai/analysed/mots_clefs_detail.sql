with v_ees as (
    select * from {{ ref('stg_ees') }} 
),

v_rpt as (
    select * from {{ ref('stg_rpt') }} 
),

v_organisation as (
    select * from {{ ref('stg_organisation') }} 
),

v_net_organisation as (
    select * from {{ ref('net_organisation') }} 
),

v_grlieu as (
    select * from {{ ref('stg_grlieu') }} 
),

v_detail_grlieu as (
    select * from {{ ref('stg_detail_grlieu') }} 
),

v_lieu as (
    select * from {{ ref('acc_rpt') }}
),

v_mots_clefs as (
    select * from {{ ref('stg_mots_clefs') }} 
),

v_export_equipement as (
    select * from {{ ref('export_equipement') }} 
),

v_int_ees_xpeage as (
    select distinct ees_id from  {{ ref('int_ees_xpeage') }}
),

v_net_regle as (
    select * from  {{ ref('net_regle') }}
),

v_agora as (
    select * from {{ ref('net_equipement_agora') }}
),
v_si_immo as (
    select * from {{ ref('net_lieux') }}
),

final as (
        select m.mot_clef,
            case when m.elo_id is null then 1 else 0 end elo_id_null,
            case when m.ees_id is null then 1 else 0 end ees_id_null,
            case when m.organisation_id is null then 1 else 0 end org_id_null,
            case when m.code_gmao is not null then 1 else 0 end code_gmao_not_null,
            case when m.elo_id not like '7%'
                    and m.elo_id not like '199%'
                then 0 else 1 end lieu_bus,
            case when upper(e.ees_libl) like '%NPU%' 
                    or e.ees_libl like '*%' 
                    or upper(e.ees_libl) like '%NE PLUS UTIL%' then 1 else 0 end ees_npu,
            case when x.ees_id is not null  then 1 else 0 end ees_xpeage,
            case when o.organisation_id is null and m.organisation_id is not null then 1 else 0 end org_abs,
            case when oo.organisation_id is null and m.organisation_id is not null then 1 else 0 end org_non_reprise,
            case when l.elo_id is null and m.elo_id is not null then 1 else 0 end rpt_abs,
            case when im.elo_id is null and l.elo_id is not null then 1 else 0 end si_immo_abs,
            case when e.ees_id is null and m.ees_id is not null then 1 else 0 end ees_abs,
            case when nr.regle_id is null and m.regle_id is not null then 1 else 0 end regle_abs,
            case when m.mots_clefs_gmao like '%M2E%' OR m.mots_clefs_gmao like 'GMAO' then 1 else 0 end gmao_m2e,
            case when ag.identifiant is null then 1 else 0 end agora_abs,
            case when ee.identifiant is null then 1 else 0 end mc_non_repris
        from v_mots_clefs m
        left join v_rpt l on m.elo_id = l.elo_id 
        left join v_ees e on m.ees_id = e.ees_id 
        left join v_organisation o on m.organisation_id = o.organisation_id
        left join v_export_equipement ee on ee.identifiant=m.mot_clef
        left join v_net_regle nr on  nr.regle_id=m.regle_id
        left join v_int_ees_xpeage x on e.ees_id=x.ees_id
        left join v_agora ag on ag.identifiant=m.mot_clef
        left join v_si_immo im on im.elo_id=m.elo_id
        left join v_net_organisation oo on m.organisation_id = oo.organisation_id
)

select * from final