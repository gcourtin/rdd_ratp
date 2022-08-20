with v_ees as (
    select * from {{ ref('stg_ees') }} 
),

v_rpt as (
    select * from {{ ref('stg_rpt') }} 
),

v_organisation as (
    select * from {{ ref('stg_organisation') }} 
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

final as (
        select m.mot_clef,
            case when m.elo_id is null then 1 else 0 end elo_id_null,
            case when m.ees_id is null then 1 else 0 end ees_id_null,
            case when m.organisation_id is null then 1 else 0 end org_id_null,
            case when m.code_gmao is not null then 1 else 0 end code_gmao_not_null,
            case when m.elo_id not like '7%'
                    and m.elo_id not like '199%'
                then 1 else 0 end lieu_bus,
            case when upper(e.ees_libl) like '%NPU%' 
                    or e.ees_libl like '*%' 
                    or upper(e.ees_libl) like '%NE PLUS UTIL%' then 1 else 0 end ees_npu,
            case when e.ees_id is null and m.ees_id is not null then 1 else 0 end ees_abs,
            case when o.organisation_id is null and m.organisation_id is not null then 1 else 0 end org_abs,
            case when l.elo_id is null and m.elo_id is not null then 1 else 0 end rpt_abs,
            case when ee.identifiant is null then 1 else 0 end mc_non_repris
        from v_mots_clefs m
        left join v_rpt l on m.elo_id = l.elo_id 
        left join v_ees e on m.ees_id = e.ees_id 
        left join v_organisation o on m.organisation_id = o.organisation_id
        left join v_export_equipement ee on ee.identifiant=m.mot_clef
)

select * from final