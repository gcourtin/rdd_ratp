with v_stg_propagations as (
    select * from {{ ref('stg_propagations') }}
),

final as (
    Select *
    from v_stg_propagations
)

select * from final