with v_stg_reseau_fonction as (
    select * from {{ ref('stg_reseau_fonction') }}
),

v_stg_reseau as (
    select * from {{ ref('net_reseau') }}
),

final as (
    Select rf.*
    from v_stg_reseau_fonction rf
    inner join v_stg_reseau r on r.reseau_id=rf.reseau_id
)

select * from final