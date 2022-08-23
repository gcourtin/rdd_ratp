with v_fonction_ees as (
    select * from {{ ref('stg_fonction_ees') }}
),

v_fonction as (
    select * from {{ ref('net_fonction') }}
),
v_reseau_fonction as (
    select * from {{ ref('stg_reseau_fonction') }}
),

v_reseau as (
    select * from {{ ref('stg_reseau') }}
),

final as (
    Select 
         fe.ees_id,listagg(distinct f.fonction_libc,',') fonction_libc
         from  v_fonction_ees fe
         inner join v_reseau_fonction rf on fe.fonction_id=rf.fonction_id
         inner join v_reseau r on r.reseau_id=rf.reseau_id
         inner join v_fonction f on f.fonction_id = rf.fonction_id and fe.fonction_id=f.fonction_id       
         where  r.reseau_libc  ='Merlin'
         group by fe.ees_id
         having listagg(distinct f.fonction_libc,',') <> '' and listagg(distinct f.fonction_libc,',') is not null
)

select * from final