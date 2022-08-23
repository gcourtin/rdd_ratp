with v_fonction_ees as (
    select * from {{ ref('stg_fonction_ees') }}
),

v_net_ees as (
    select * from {{ ref('net_ees') }}
),

v_net_fonction as (
    select * from {{ ref('net_fonction') }}
),


final as (
    Select 
        fe.*
        from v_fonction_ees fe
        inner join v_net_ees e on fe.ees_id=e.ees_id
        inner join v_net_fonction nf on fe.fonction_id=nf.fonction_id       
)

select * from final