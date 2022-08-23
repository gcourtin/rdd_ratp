with v_net_anomalie as (
    select * from  {{ ref('net_anomalie') }} 
 ),

final as (
    select
        a.anomalie_id as identifiant,
        a.anomalie_libl as Libelle,
        null as description,
        a.ees_id as Type_d_equipement,
        true as actif,
        a.anomalie_niv_urg as niveau_d_urgence,
        a.anomalie_es_hs as etat_de_l_equipement,
        a.anomalie_inacceptable as situation_inacceptable,
        a.anomalie_nettete as critere_de_nettete,
        case when a.anomalie_niv_urg = 9 then 1 else 0 end  as piece_jointe_obligatoire,
        null as verifications_exploitant,
        null as sensible
    from v_net_anomalie a
)

select * from final

