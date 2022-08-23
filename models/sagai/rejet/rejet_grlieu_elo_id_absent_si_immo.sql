with v_stg_grlieu as (
    select * from  {{ ref('stg_grlieu') }}
),

v_ext_lieu_si_immo as (
    select * from  {{ ref('ext_lieu_si_immo') }}
),

v_stg_rpt as (
    select * from  {{ ref('stg_rpt') }}
),

final as (
    select a.grlieu_id,a.elo_id,case when b.elo_id is null then 'Absent de rpt' else 'Présent dans rpt '||b.elo_libc end commentaire 
from v_stg_grlieu a
left join v_ext_lieu_si_immo c on a.elo_id =c.elo_id
left join v_stg_rpt b on a.elo_id=b.elo_id 
where c.elo_id is null and a.elo_id not like '7%' and a.elo_id not like '199%'
)

select * from final