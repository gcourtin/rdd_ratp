with v_propagations as (
    select * from {{ ref('net_propagations') }}
),

final as (
    select 
        *,
        case 
            when max_typ_trans < 2 then '-- Néant --'
            when contains(propagation_typ_trans,'5') then 'Par Web services'            
            when contains(propagation_typ_trans,'3') then 'Par trame-mail vers les GMAO'
            when contains(propagation_typ_trans,'4') then 'Par e-mail en format texte ou html'
            when contains(propagation_typ_trans,'6') then 'Par e-mail avec URL action (nouveauté du marché propreté 2021)'
            when contains(propagation_typ_trans,'2') then 'Par imprimante (interne RATP)'
            else '-- Néant --'
        end  type_propagation_txt,
        case 
            when max_typ_trans < 2 then 0
            when contains(propagation_typ_trans,'5') then 5            
            when contains(propagation_typ_trans,'3') then 2
            when contains(propagation_typ_trans,'4') then 4
            when contains(propagation_typ_trans,'6') then 6
            when contains(propagation_typ_trans,'2') then 2
            else 0
        end  type_propagation_num
    from (
        select 
            propagation_id,
            propagation_degrade,
            max(propagation_typ_trans) max_typ_trans,
            listagg(distinct propagation_action,',') propagation_action, 
            listagg(distinct propagation_typ_trans,',') propagation_typ_trans
        from v_propagations
        group by propagation_id,propagation_degrade
        order by propagation_id,propagation_degrade
        )
)

select * from final