with v_stg_grlieu as (
    select * from {{ ref('net_grlieu') }}
),
v_acc_lieux as (
    select * from {{ ref('acc_lieux') }}
),

final as (
     Select 
        r.elo_id ,
        gr.grlieu_id
     from
	     v_stg_grlieu gr
         left join v_acc_lieux r on r.lieu_id = gr.elo_id
)

select * from final