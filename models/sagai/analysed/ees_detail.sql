with v_ees as (
    select * from {{ ref('stg_ees') }} 
),

v_regle_ees as (
    select * from {{ ref('stg_regle_ees') }} 
),

v_fonction_ees as (
    select * from {{ ref('stg_fonction_ees') }}
),

v_mots_clefs as (
    select * from {{ ref('stg_mots_clefs') }} 
),

v_bloc as (
    select * from {{ ref('stg_bloc') }}     
),

v_depeche_last_date as (
    select * from {{ ref('depeche_last_upd') }}
),

v_anomalie as (
    select * from {{ ref('stg_anomalie') }}
),

v_ees_generique as (
    select * from {{ ref('stg_ees_generique') }}
),

v_ees_mots_clefs as (
    select ees_id,count(*) nb from v_mots_clefs group by ees_id
),

v_ees_fonction as (
    select ees_id,count(*) nb from v_fonction_ees group by ees_id
),

v_ees_regle as (
    select ees_id,count(*) nb from v_regle_ees group by ees_id
),

v_ees_bloc as (
    select bloc_ees_id,count(concat(v.depeche_date,v.depeche_num)) nb,max(nvl(v.last_action_date,to_date('1900-01-01'))) as derniere_depeche from v_bloc b
    inner join v_depeche_last_date v on b.depeche_date=v.depeche_date  and b.depeche_num =v.depeche_num  group by bloc_ees_id
),

v_ees_anomalie as (
    select ees_id,count(*) nb from v_anomalie group by ees_id
),
v_ees_generic as (
    select ees_id,count(*) nb from v_ees_generique group by ees_id
),

v_export_ees as (
    select * from {{ ref('export_ees') }}
),

v_net_fonction_merlin as (
    select * from {{ ref('net_fonction_merlin') }}
),

final as (
    select
        e.ees_id,
        max(b.derniere_depeche) derniere_depeche,
        max(case when a.ees_id is null then 0 else 1 end) avec_ano,
        max(case when b.bloc_ees_id is null then 0 else 1 end) avec_bloc,
        max(case when g.ees_id is null then 0 else 1 end) avec_generic,
        max(case when f.ees_id is null then 0 else 1 end) avec_fonction,
        max(case when m.ees_id is null then 0 else 1 end) avec_equip,
        max(case when r.ees_id is null then 0 else 1 end) avec_regle,
        max(case when e.ees_id is null or upper(e.ees_libl) like '%NPU%' or e.ees_libl like '*%' or upper(ees_libl) like '%NE PLUS UTIL%'
        then 1
        else 0
        end) as ees_npu,
        max(case when nf.ees_id is null and f.ees_id is not null then 1 else 0 end) as ees_rejete_fonction_Bus,
        max(b.nb) nb_depeche,
        max(r.nb) nb_regle,
        max(f.nb) nb_fonction,
        max(m.nb) nb_mots_clefs,
        max(case when ee.id_sagai is null then 1 else 0 end) ees_non_repris
    from v_ees e	
    left join v_ees_anomalie a on a.ees_id=e.ees_id
    left join v_ees_bloc b on b.bloc_ees_id=e.ees_id
    left join v_ees_generique	g on g.ees_id=e.ees_id
    left join v_ees_fonction f on f.ees_id=e.ees_id
    left join v_ees_mots_clefs m on m.ees_id=e.ees_id
    left join v_ees_regle r on r.ees_id=e.ees_id
    left join v_export_ees ee on ee.id_sagai=e.ees_id 
    left join v_net_fonction_merlin nf on nf.ees_id=e.ees_id
    group by e.ees_id   
)

select * from final