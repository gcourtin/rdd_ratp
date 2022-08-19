with v_ees as (
    select * from {{ ref('net_ees') }}
),

v_fonction_ees as (
    select * from {{ ref('net_fonction_ees') }} 
),

v_reseau_fonction as (
    select * from {{ ref('net_reseau_fonction') }} 
),

v_fonction as (
    select * from {{ ref('net_fonction') }} 
),

final as (
    select e.ees_id, f.fonction_libc as x_peage
    from  v_ees e
    inner join v_fonction_ees fe on e.ees_id = fe.ees_id
    inner join v_fonction f on f.fonction_id = fe.fonction_id
    inner join v_reseau_fonction rf on rf.fonction_id = f.fonction_id
    where rf.reseau_id = 6
)

select * from final