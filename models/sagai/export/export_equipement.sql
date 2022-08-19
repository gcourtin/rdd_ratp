with v_equipement_ratpi as (
    select * from {{ ref('int_equipement_ratpi') }}
),

v_equipement_inventaire as (
    select * from {{ ref('int_equipement_inventaire') }}
),

v_equipement_autres as (
    select * from {{ ref('int_equipement_autres') }}
),

v_equipement_agora as (
    select * from {{ ref('net_equipement_agora') }}
),

final as (
    select 
        identifiant,nom,actif,equipement_asservi,code_gmao,responsable,lieu,
        ees,organisation,xpeage,source,description,statut,regle,code_mire
    from v_equipement_agora
    union all
    select 
        identifiant,nom,actif,equipement_asservi,code_gmao,responsable,lieu,
        ees,organisation,xpeage,source,description,statut,regle,code_mire    
    from v_equipement_ratpi 
    where identifiant not in (
        select identifiant from v_equipement_agora
        )
    union all
    select 
        identifiant,nom,actif,equipement_asservi,code_gmao,responsable,lieu,
        ees,organisation,xpeage,source,description,statut,regle,code_mire    
    from v_equipement_inventaire
    where identifiant not in (
        select identifiant from v_equipement_agora
        union all
        select identifiant from v_equipement_ratpi 
        )
    union all
    select 
        identifiant,nom,actif,equipement_asservi,code_gmao,responsable,lieu,
        ees,organisation,xpeage,source,description,statut,regle,code_mire    
    from v_equipement_autres
    where identifiant not in (
        select identifiant from v_equipement_agora
        union all
        select identifiant from v_equipement_ratpi 
        union all
        select identifiant from v_equipement_inventaire
        )

)

select * from final