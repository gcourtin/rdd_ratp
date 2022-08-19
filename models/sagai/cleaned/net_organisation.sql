with v_organisation as (
    select * from {{ ref('stg_organisation') }}
),

v_propagation as (
    select * from {{ ref('net_propagations') }}
),

v_mapping_org_dept as (
    select * from {{ ref('map_org_dept') }}
),

final as (
    Select 
        o.organisation_id,
        o.organisation_id_pere,
        o.organisation_nom,
        o.organisation_type,
        o.organisation_code_refpat,
        o.organisation_typ_trans,
        o.organisation_typ_deg,
        case 
            when length(to_varchar(replace(replace(replace(trim(o.organisation_contacts),'.'),' '),'-')))=10 or length(to_varchar(replace(replace(replace(trim(o.organisation_contacts),'.'),' '),'-')))=9
             then to_varchar(replace(replace(replace(trim(o.organisation_contacts),'-'),'.'),' '))
             else 
                case when o.organisation_tel like '%-' then substring(o.organisation_tel,1,length(o.organisation_tel)-1) else trim(o.organisation_tel) end
        end as organisation_tel,
        o.organisation_fax,
        o.organisation_imp,
        o.organisation_typ_ema,
        o.organisation_ema,
        o.organisation_ema_sujet,
        o.organisation_adr,
        o.propagation_id,
        o.organisation_num_etat,
        o.organisation_autopropa,
        o.organisation_nftf_auto,
        o.organisation_lieu_id,
        o.organisation_sagai2,
        o.zonegeo_id,
        o.grlieu_id,
        o.organisation_impr_auto,
        case 
            when length(to_varchar(replace(replace(replace(trim(o.organisation_contacts),'.'),' '),'-')))=10 or length(to_varchar(replace(replace(replace(trim(o.organisation_contacts),'.'),' '),'-')))=9
             then to_varchar(replace(replace(replace(trim(o.organisation_contacts),'-'),'.'),' '))
             else 
                case when o.organisation_contacts like '%-' then substring(o.organisation_contacts,1,length(o.organisation_contacts)-1) else trim(o.organisation_contacts) end
        end as organisation_contacts,
        o.organisation_dbl_emis,
        o.organisation_dbl_depeche,
        o.fonction_id,
        o.organisation_ema_nbmax,
        o.organisation_date_cre,
        o.organisation_utilisateur_cre,
        o.organisation_date_modif,
        o.organisation_utilisateur_modif,
        o.organisation_ano_visi,
        o.organisation_com
        from v_organisation o
        inner join (select distinct propagation_id from v_propagation) p on o.propagation_id=p.propagation_id
        where 
            upper(o.organisation_id) not like '%Z-REFOR%'
        and upper(o.organisation_id_pere) not like '%Z-REFOR%'
        and upper(o.organisation_nom) not like '%NPU%'
        and upper(o.organisation_id_pere) not like '%METEO%'
        and upper(o.organisation_id) not like '%GUEST%' 
    union all
    select distinct
        'DEPARTEMENT_'||departement as organisation_id,
        null as organisation_id_pere,
        'DEPARTEMENT_'||departement as organisation_nom,
        null as organisation_type,
        null as organisation_code_refpat,
        null as organisation_typ_trans,
        null as organisation_typ_deg,
        null as organisation_tel,
        null as organisation_fax,
        null as organisation_imp,
        null as organisation_typ_ema,
        null as organisation_ema,
        null as organisation_ema_sujet,
        null as organisation_adr,
        null as propagation_id,
        null as organisation_num_etat,
        null as organisation_autopropa,
        null as organisation_nftf_auto,
        null as organisation_lieu_id,
        null as organisation_sagai2,
        null as zonegeo_id,
        null as grlieu_id,
        null as organisation_impr_auto,
        null as organisation_contacts,
        null as organisation_dbl_emis,
        null as organisation_dbl_depeche,
        null as fonction_id,
        null as organisation_ema_nbmax,
        null as organisation_date_cre,
        null as organisation_utilisateur_cre,
        null as organisation_date_modif,
        null as organisation_utilisateur_modif,
        null as organisation_ano_visi,
        null as organisation_com
    from
        v_mapping_org_dept
    where
        departement is not null
)

select * from final