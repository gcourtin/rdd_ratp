with v_lieu as (
    select * from  {{ ref('net_lieux') }}
),

v_rpt as (
    select * from {{ ref('stg_rpt') }}
),

final as (
    Select 
         r.*
         from v_rpt r
         inner join v_lieu l on r.elo_id=l.elo_id
)

select * from final