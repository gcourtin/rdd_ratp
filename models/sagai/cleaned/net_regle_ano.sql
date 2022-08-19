with v_regle as (
    select * from {{ ref('net_regle') }}
 ),

 v_anomalie as (
    select * from {{ ref('net_anomalie') }}
),

 v_regle_ano as (
     select * from  {{ ref('stg_regle_ano') }}
 ),

final as (
    Select 
         ra.* 
         from  v_regle_ano ra
         inner join v_regle r on ra.regle_id=r.regle_id
         inner join v_anomalie a on a.anomalie_id=ra.anomalie_id
)

select * from final