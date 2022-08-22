with v_stg_fonction as (
    select * from {{ ref('stg_fonction') }}
),

final as (
    Select *
    from v_stg_fonction
    where fonction_libc not like 'BUS%'
)

select * from final