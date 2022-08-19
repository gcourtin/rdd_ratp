with v_ees as (
    select * from {{ ref('net_ees') }} 
 ),

 v_anomalie as (
     select * from {{ ref('stg_anomalie') }} 
 ),

 final as (
     Select a.*
        from v_anomalie a
        inner join v_ees e on e.ees_id=a.ees_id
 )

 select * from final