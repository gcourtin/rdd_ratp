with v_rejet_ees as (
    select * from {{ ref('rejet_ees') }} 
),

v_mots_clefs_detail as (
    select * from  {{ ref('mots_clefs_detail') }}
),

v_mots_clefs as (
    select * from  {{ ref('stg_mots_clefs') }}
),


final as (
    select
        md.mot_clef,
        m.ees_id,
        case              
            when elo_id_null=1 then 'Equipement sans lieu'
            when ees_id_null=1 then 'Equipement sans EES'
            when lieu_bus = 1 then 'Equipement rattaché à un lieu BUS'            
            when org_id_null=1 then 'Equipement sans intervenant principal'
            when ees_abs = 1 then 'Equipement rattaché à un EES inconnu'
	        when org_abs = 1 then 'Equipement rattaché à une organisation inconnue'
	        when rpt_abs = 1 then 'Equipement rattaché à un lieu inconnu'
            when re.ees_id is not null then 'Equipement rattaché à un '||re.motif   
            when ees_xpeage = 1 then 'Equipement lié au réseau XPEAGE'                  
            else 'Raison Inconnu'
        end as motif
    from v_mots_clefs_detail md
    inner join v_mots_clefs m on m.mot_clef=md.mot_clef
    left join v_rejet_ees re on re.ees_id=m.ees_id
    where mc_non_repris=1
)

select * from final