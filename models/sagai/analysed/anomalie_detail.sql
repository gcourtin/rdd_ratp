with v_ees as (
    select * from {{ ref('stg_ees') }} 
),

v_regle_ano as (
    select * from {{ ref('stg_regle_ano') }} 
),

v_anomalie as (
    select * from {{ ref('stg_anomalie') }} 
),

v_export_ees as (
    select * from {{ ref('export_ees') }} 
),

v_export_anomalie as (
    select * from {{ ref('export_anomalie') }} 
),

v_ano_regle as (select anomalie_id,count(*) nb from v_regle_ano group by anomalie_id),

final as (
    select *, ((ees_present+regle_ano)=0) ano_inutilise
    from (
        select a.anomalie_id,
        case when e.ees_id is null then 0 else 1 end ees_present,
        case when a.ees_id is null then 1 else 0 end ees_id_null,
        case when r.anomalie_id is null then 0 else 1 end regle_ano,
        case when ee.id_sagai is null and e.ees_id is not null then 1 else 0 end ees_non_repris,
        case when ea.identifiant is null then 1 else 0 end ano_non_reprise,
        r.nb nb_regle_ano
        from v_anomalie a
        left join v_ees e on a.ees_id=e.ees_id 
        left join v_ano_regle r on r.anomalie_id = a.anomalie_id
        left join v_export_ees ee on ee.id_sagai = a.ees_id
        left join v_export_anomalie ea on ea.identifiant=a.anomalie_id
        )
)

select * from final