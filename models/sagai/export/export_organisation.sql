with v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_mapping_org_dept as (
    select * from {{ ref('net_map_org_dept') }}
),

v_propagations as (
    select * from {{ ref('int_propagations') }}
),

final as (
    Select
        o.organisation_id as Identifiant,
        o.organisation_nom as Libelle,
        case 
            when o.organisation_id=o.organisation_id_pere and d.departement is not null then 'DEPARTEMENT_'||d.departement
            when o.organisation_id=o.organisation_id_pere and d.departement is null then null
            else o.organisation_id_pere
        end as Organisation_mere,
        case 
            when d.departement is not null then 'RATP' 
            when contains(o.organisation_id,'/') then split_part(o.organisation_id,'/',1)
            else o.organisation_nom
        end as Societe_mere,
        case
            when d.departement='PG' then null
            when o.organisation_id in ('DEPARTEMENT_TRAM', 'DEPARTEMENT_RRE', 'DEPARTEMENT_SEM', 'DEPARTEMENT_SUR',
             'DEPARTEMENT_RATP Smart Systems', 'DEPARTEMENT_RER', 'DEPARTEMENT_RDS', 'DEPARTEMENT_MTS', 'DEPARTEMENT_MOP', 'DEPARTEMENT_CML') 
             then 'DEPARTEMENT_EXPLOITANT'
            when o.organisation_id in ('DEPARTEMENT_M2E', 'DEPARTEMENT_SIT', 'DEPARTEMENT_MRF', 'DEPARTEMENT_RATP-I')
             then 'DEPARTEMENT_INTERVENANT'
            else d.organisation_type
         end as Type_d_organisation,
        case
            when o.organisation_contacts=o.organisation_tel or contains(o.organisation_contacts,o.organisation_tel)
             then to_varchar(o.organisation_contacts)
            when o.organisation_contacts is null and o.organisation_tel is null
             then null
            when o.organisation_contacts is null or o.organisation_tel is null
             then to_varchar(coalesce(o.organisation_contacts,o.organisation_tel))
            when o.organisation_contacts<>o.organisation_tel
             then to_varchar(o.organisation_contacts||'/'||o.organisation_tel)
            else null
        end as Tel_intervenant,
        true as Actif,         
        case
            when o.organisation_dbl_emis is null and o.organisation_typ_deg then null
            when o.organisation_dbl_emis=1 then 'Double_emission'
            when o.organisation_typ_deg=1 then 'Mode_degrade'
            else 'Mode nominal'
        end as Mode_de_propagation_par_defaut,
        null as Mode_de_propagation_force,
        null as Date_de_debut,
        null as Date_de_fin,
        replace(o.organisation_adr,'|',chr(10)) as Adresse_postale,
        case
            when o.organisation_dbl_emis=1 or o.organisation_typ_deg=0 then p_deg0.type_propagation_txt else null
        end as Mode_de_propagation_nominal,
        case
            when p_deg0.type_propagation_num=2 then to_varchar(o.organisation_imp)
            when p_deg0.type_propagation_num=4 then to_varchar(o.organisation_typ_ema)
            else null
        end as Mode_de_propagation_nominal_cible,
        case
            when o.organisation_dbl_emis=1 or o.organisation_typ_deg=1 then p_deg1.type_propagation_txt else null
        end as Mode_de_propagation_degrade,
        case
            when p_deg1.type_propagation_num=2 then to_varchar(o.organisation_imp)
            when p_deg1.type_propagation_num=4 then to_varchar(o.organisation_typ_ema)
            else null
        end as Mode_de_propagation_degrade_cible,
        o.organisation_com as Commmentaire
        from v_organisation o
        left join v_mapping_org_dept d on o.organisation_id=d.organisation_id
        left join v_propagations p_deg0 on o.propagation_id=p_deg0.propagation_id and p_deg0.propagation_degrade=0
        left join v_propagations p_deg1 on o.propagation_id=p_deg1.propagation_id and p_deg1.propagation_degrade=1
)

select * from final