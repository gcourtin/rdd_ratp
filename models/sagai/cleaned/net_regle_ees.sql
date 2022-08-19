with v_regle as (
    select * from {{ ref('net_regle') }}
),

v_ees as (
    select * from {{ ref('net_ees') }}
),

v_regle_ees as (
     select * from  {{ ref('stg_regle_ees') }}
),

final as (
    Select 
         re.* 
         from  v_regle_ees re
         inner join v_regle r on re.regle_id=r.regle_id
         inner join v_ees e on e.ees_id=re.ees_id
)

select * from final