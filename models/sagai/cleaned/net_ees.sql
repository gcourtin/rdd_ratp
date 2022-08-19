with v_ees as (
    select * from {{ ref('stg_ees') }}
),

final as (
    Select 
        *
        from v_ees   
         where 
             upper(ees_libl) not like '%NPU%' 
         and ees_libl not like '*%' 
         and upper(ees_libl) not like '%NE PLUS UTIL%' 

)

select * from final