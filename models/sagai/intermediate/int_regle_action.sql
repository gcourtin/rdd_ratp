with v_net_regle_action as (
    select * from {{ ref('net_regle_action') }}
 ),

final as (
     select 
        regle_id,
        listagg(distinct organisation_id,',') organisation_id
     from v_net_regle_action
     group by regle_id
)

select * from final