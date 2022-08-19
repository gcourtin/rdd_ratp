with v_net_lieux as (
    select * from  {{ ref('net_lieux') }} 
),

v_regle as (
    select * from  {{ ref('net_regle') }}
),

v_regle_lieu as (
     select* from {{ ref('stg_regle_lieu') }}
),

final as (
    Select 
         rl.* 
         from v_regle_lieu rl
         inner join v_regle r on rl.regle_id=r.regle_id
         inner join v_net_lieux nl on nl.elo_id=rl.refpat112_id
)

select * from final