with v_ees as (
    select * from {{ ref('stg_ees') }}
),

v_net_fonction_merlin as (
    select * from {{ ref('net_fonction_merlin') }}
),

final as (
    Select 
        e.*
        from v_ees e
        inner join v_net_fonction_merlin f on f.ees_id = e.ees_id    
         and upper(ees_libl) not like '%NPU%' 
         and ees_libl not like '*%' 
         and upper(ees_libl) not like '%NE PLUS UTIL%' 
)

select * from final