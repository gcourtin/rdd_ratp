with v_stg_grlieu as (
    select * from {{ ref('net_grlieu') }}
),
v_net_lieux as (
    select * from {{ ref('net_lieux') }}
),

final as (
     Select 
        gr.grlieu_id,
        listagg(distinct gr.elo_id,',') elo_id
     from
	     v_stg_grlieu gr
         inner join v_net_lieux r on r.elo_id = gr.elo_id
     group by gr.grlieu_id
)

select * from final