with v_bloc as (
    select * from {{ ref('stg_bloc') }} 
),

v_evenement as (
    select * from {{ ref('stg_evenement') }} 
),

v_regle_action as (
    select * from {{ ref('stg_regle_action') }} 
),

v_regle_copie as (
    select * from {{ ref('stg_regle_copie') }} 
),

v_regle as (
    select * from {{ ref('stg_regle') }} 
),

v_organisation as (
    select * from {{ ref('stg_organisation') }}     
),

v_utilisateur as (
    select * from {{ ref('stg_utilisateur') }}     
),

v_organisation_reseau as (
    select * from {{ ref('stg_organisation_reseau') }}     
),

v_organisation_zone as (
    select * from {{ ref('stg_organisation_zone') }}     
),

v_mots_clefs as (
    select * from {{ ref('stg_mots_clefs') }} 
),

v_propagations as (
    select distinct propagation_id from {{ ref('stg_propagations') }} 
),

v_depeche_last_date as (
    select * from {{ ref('depeche_last_upd') }}
),

v_export_organisation as (
    select * from {{ ref('export_organisation') }} 
),

v_org_regle as (
    select organisation_id,count(*) nb from v_regle group by organisation_id
),

v_org_organisation_reseau as (
    select or_organisation_id,count(*) nb from v_organisation_reseau group by or_organisation_id
),

v_org_organisation_zone as (
    select oz_organisation_id,count(*) nb from v_organisation_zone group by oz_organisation_id
),

v_mot_clef_org_id as (
    select organisation_id,count(*) nb from v_mots_clefs group by organisation_id
),

v_org_regle_action as (
    select organisation_id,count(*) nb from v_regle_action group by organisation_id
),

v_org_regle_copie as (
    select organisation_id,count(*) nb from v_regle_copie group by organisation_id
),

v_org_utilisateur_org_id as (
    select utilisateur_org_id,count(*) nb from v_utilisateur group by utilisateur_org_id
),

v_org_utilisateur as (
    select organisation_id,count(*) nb from v_utilisateur group by organisation_id
),

v_evt_org_enr as (
    select evt_org_enr,count(*) nb from v_evenement group by evt_org_enr    
),

v_org_bloc_enr as (
    select bloc_org_enr,count(concat(v.depeche_date,v.depeche_num)) nb,max(nvl(v.last_action_date,to_date('1900-01-01'))) as derniere_depeche from v_bloc b
    inner join v_depeche_last_date v on b.depeche_date=v.depeche_date  and b.depeche_num =v.depeche_num  group by bloc_org_enr
),

v_org_bloc_id as (
    select bloc_org_id,count(concat(v.depeche_date,v.depeche_num)) nb,max(nvl(v.last_action_date,to_date('1900-01-01'))) as derniere_depeche from v_bloc b
    inner join v_depeche_last_date v on b.depeche_date=v.depeche_date  and b.depeche_num =v.depeche_num  group by bloc_org_id
),

final as (
    select *,
        case when bloc_org_enr+bloc_org_id+mot_clef_org_id+org_reseau+org_zone+org_regle+org_regle_act+org_regle_copie+org_user+org_user_org_id+evt_org_enr=0
        then 1
        else 0
        end as nb_inutilise
    from (
        select distinct
            o.organisation_id organisation_id,
            case when b1.bloc_org_enr is null then 0 else 1 end bloc_org_enr,
            case when b2.bloc_org_id is null then 0 else 1 end bloc_org_id,
            case when e1.evt_org_enr is null then 0 else 1 end evt_org_enr,
            case when m1.organisation_id is null then 0 else 1 end mot_clef_org_id,
            case when or1.or_organisation_id is null then 0 else 1 end org_reseau,
            case when oz1.oz_organisation_id is null then 0 else 1 end org_zone,	
            case when r1.organisation_id is null then 0 else 1 end org_regle,
            case when ra1.organisation_id is null then 0 else 1 end org_regle_act,
            case when rc1.organisation_id is null then 0 else 1 end org_regle_copie,
            case when u1.organisation_id is null then 0 else 1 end org_user,
            case when u2.utilisateur_org_id is null then 0 else 1 end org_user_org_id,
            case when p.propagation_id is null and o.propagation_id is not null then 1 else 0 end propagation_absente,
            b1.nb		nb_bloc_org_enr,
            b1.derniere_depeche last_bloc_org_enr,
            b2.nb   	nb_bloc_org_id,
            b2.derniere_depeche last_bloc_org_id,
            e1.nb   	nb_evt_org_enr,
            m1.nb   	nb_mot_clef_org_id,
            or1.nb  	nb_org_reseau,
            oz1.nb  	nb_org_zone,	
            r1.nb   	nb_org_regle,
            ra1.nb  	nb_org_regle_act,
            rc1.nb  	nb_org_regle_copie,
            u1.nb   	nb_org_user,
            u2.nb   	nb_org_user_org_id,
            CASE WHEN O.ORGANISATION_ID NOT LIKE '%Z-REFOR%' and o.organisation_id_pere not like '%Z-REFOR%' and o.organisation_nom not like '%NPU%' and o.organisation_id_pere not like '%METEO%' and o.organisation_id not like '%GUEST%'
            then 0
            else 1
            end as npu,
            case when o.organisation_id not like '%TEST%' then 0 else 1 end as test,
            case when eo.identifiant is null then 1 else 0 end org_non_repris
        from v_organisation o
        left join v_org_bloc_enr b1 on o.organisation_id = b1.bloc_org_enr
        left join v_org_bloc_id b2 on o.organisation_id = b2.bloc_org_id
        left join v_evt_org_enr e1 on o.organisation_id = e1.evt_org_enr
        left join v_mot_clef_org_id m1 on o.organisation_id = m1.organisation_id 
        left join v_org_organisation_reseau or1 on o.organisation_id = or1.or_organisation_id
        left join v_org_organisation_zone oz1 on o.organisation_id = oz1.oz_organisation_id
        left join v_org_regle r1 on o.organisation_id = r1.organisation_id
        left join v_org_regle_action ra1 on o.organisation_id = ra1.organisation_id
        left join v_org_regle_copie rc1 on o.organisation_id = rc1.organisation_id
        left join v_org_utilisateur u1 on o.organisation_id = u1.organisation_id
        left join v_org_utilisateur_org_id u2 on o.organisation_id = u2.utilisateur_org_id
        left join v_export_organisation eo on eo.identifiant = o.organisation_id
        left join v_propagations p on p.propagation_id = o.propagation_id
        )
)

select * from final