with v_stg_grlieu as (
    select * from  {{ ref('stg_grlieu') }}
),

v_stg_zonegeo as (
    select * from  {{ ref('stg_zonegeo') }}
),

v_export_lieux as (
    select * from  {{ ref('export_lieux') }}
),

v_export_groupe_de_lieux as (
    select * from  {{ ref('export_groupe_de_lieux') }}
),

v_ext_lieu_si_immo as (
    select * from  {{ ref('ext_lieu_si_immo') }}
),

v_stg_rpt as (
    select * from  {{ ref('stg_rpt') }}
),

detail as (
    select a.grlieu_id,
    max(case when a.elo_id like '7%' or a.elo_id like '199%' then 1 else 0 end) contient_lieux_bus,
    min(case when a.elo_id like '7%' or a.elo_id like '199%' then 1 else 0 end) contient_que_lieux_bus,
    max(case when g.elo_id is null then 1 else 0 end) contient_lieux_inex_rpt,
    max(case when f.elo_id is null then 1 else 0 end) contient_lieux_inex_si_immo,    
    min(case when f.elo_id is null then 1 else 0 end) contient_que_lieux_inex_si_immo,
    max(case when e.grlieu_id is null then 1 else 0 end) pas_zonegeo_associee,
    max(case when c.eloid is null then 1 else 0 end) contient_lieux_inex_lieu,
    min(case when c.eloid is null then 1 else 0 end) contient_que_lieux_inex_lieu
    from v_stg_grlieu a
    left join v_export_groupe_de_lieux b on b.identifiant = a.grlieu_id 
    left join v_export_lieux c on a.elo_id = c.eloid 
    left join v_stg_zonegeo e on e.grlieu_id =a.grlieu_id
    left join v_ext_lieu_si_immo f on f.elo_id=a.elo_id
    left join v_stg_rpt g on g.elo_id=a.elo_id
    where b.identifiant is null
    group by a.grlieu_id
),

final as (
    select 
        grlieu_id,
        case 
           when contient_que_lieux_bus=1 then 'GRLIEU ne contenant que des lieux BUS non repris'
           when contient_que_lieux_inex_si_immo=1 then 'GRLIEU ne contenant que des lieux hors BUSnon présent dans SI_IMMO'
           when contient_que_lieux_inex_lieu=1 then 'GRLIEU ne contenant que des lieux non repris'
           else 'Raison inconnue'
        end as motif
    from detail
)

select * from final

