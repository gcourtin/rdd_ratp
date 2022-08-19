with v_net_regle_lieu as (
    select * from  {{ ref('net_regle_lieu') }} 
),

final as (
    Select 
         rl.regle_id,
         listagg(distinct rl.refpat112_id,',') elo_id
         from v_net_regle_lieu rl
         group by rl.regle_id
)

select * from final