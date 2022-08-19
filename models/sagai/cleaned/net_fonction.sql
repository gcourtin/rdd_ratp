with v_stg_fonction as (
    select * from {{ ref('stg_fonction') }}
),

final as (
    Select *
    from v_stg_fonction
)

select * from final