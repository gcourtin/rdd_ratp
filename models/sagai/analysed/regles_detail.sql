with v_ees as (
    select * from {{ ref('stg_ees') }} 
),

v_net_ees as (
    select * from {{ ref('net_ees') }}     
),

v_regle_ees as (
    select * from {{ ref('stg_regle_ees') }} 
),

v_regle_action as (
    select * from {{ ref('stg_regle_action') }} 
),

v_regle_copie as (
    select * from {{ ref('stg_regle_copie') }} 
),

v_regle_ano as (
    select * from {{ ref('stg_regle_ano') }} 
),

v_regle_lieu as (
    select * from {{ ref('stg_regle_lieu') }} 
),

v_regle as (
    select * from {{ ref('stg_regle') }} 
),

v_lieu as (
    select * from {{ ref('acc_rpt') }}
),

v_mots_clefs as (
    select * from {{ ref('stg_mots_clefs') }} 
),

v_bloc as (
    select * from {{ ref('stg_bloc') }}     
),

v_rpt as (
    select * from {{ ref('stg_rpt') }}     
),

v_anomalie as (
    select * from {{ ref('stg_anomalie') }}     
),

v_organisation as (
    select * from {{ ref('stg_organisation') }}     
),

v_net_lieux as (
    select * from {{ ref('net_lieux') }}     
),

v_export_regle as (
    select * from {{ ref('export_regle_de_routage') }}
),

v_rejet_ees as (
    select * from {{ ref('rejet_ees') }}
),

final as (
    select r.regle_id,
        max(case when ra.regle_id is null then 0 else 1 end) rano,
        max(case when re.regle_id is null then 0 else 1 end) rees,
        max(case when rl.regle_id is null then 0 else 1 end) rlei,
        max(case when rac.regle_id is null then 0 else 1 end) ract,
        max(case when rc.regle_id is null then 0 else 1 end) rcopie,
        max(case when e.ees_id is null then 1 else 0 end) ees_inex,
        max(case when e2.ees_id is null and e.ees_id is not null then 1 else 0 end) ees_npu, 
        max(case when rp.elo_id is null then 1 else 0 end) lieu_inex,
        max(case when nl.elo_id is null and rp.elo_id is not null then 1 else 0 end) lieu_bus,
        max(case when a.anomalie_id is null then 1 else 0 end) ano_inex,
        max(case when o.organisation_id is null then 1 else 0 end) org_action_inex,
        max(case when o1.organisation_id is null then 1 else 0 end) org_copie_inex,
        max(case when o2.organisation_id is null then 1 else 0 end) org_inex,        
        max(case when er.identifiant is null then 1 else 0 end) regle_non_reprise,
        max(case when rj.ees_id is not null then 1 else 0 end) ees_rejete,
        max(case when r.regle_dat_fin < current_timestamp() and r.regle_dat_fin is not null then 1 else 0 end) dat_fin,
        rano+rees+rlei as rall
    from v_regle r
        left join v_regle_ano ra on r.regle_id =ra.regle_id
        left join v_regle_ees re on r.regle_id =re.regle_id
        left join v_regle_lieu rl on r.regle_id =rl.regle_id
        left join v_regle_action rac on r.regle_id =rac.regle_id
        left join v_regle_copie rc on r.regle_id =rc.regle_id
        left join v_ees e on e.ees_id =re.ees_id
        left join v_net_ees e2 on e2.ees_id =re.ees_id        
        left join v_rpt rp on rp.elo_id=rl.refpat112_id
        left join v_net_lieux nl on nl.elo_id=rl.refpat112_id        
        left join v_anomalie  a on ra.anomalie_id =a.anomalie_id
        left join v_organisation o on rac.organisation_id = o.organisation_id
        left join v_organisation o1 on rc.organisation_id = o1.organisation_id
        left join v_organisation o2 on r.organisation_id = o2.organisation_id        
        left join v_export_regle er on er.identifiant=r.regle_id
        left join v_rejet_ees rj on rj.ees_id=e.ees_id
    group by r.regle_id
)

select * from final