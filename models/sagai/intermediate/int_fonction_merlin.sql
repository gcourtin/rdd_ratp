with v_fonction_ees as (
    select * from {{ ref('net_fonction_ees') }}
),

v_fonction as (
    select * from {{ ref('net_fonction') }}
),

v_reseau as (
    select * from {{ ref('net_reseau') }}
),

v_reseau_fonction as (
    select * from {{ ref('net_reseau_fonction') }}
),

final as (
    Select 
         distinct fe.ees_id,f.fonction_libc 
         from  v_fonction_ees fe
         inner join v_reseau_fonction rf on fe.fonction_id=rf.fonction_id
         inner join v_reseau r on r.reseau_id=rf.reseau_id
         inner join v_fonction f on f.fonction_id = rf.fonction_id and fe.fonction_id=f.fonction_id       
         where  r.reseau_libc  ='Merlin'
         order by fe.ees_id,f.fonction_libc
)

select * from final