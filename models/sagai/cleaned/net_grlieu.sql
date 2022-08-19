with v_stg_grlieu as (
    select * from {{ ref('stg_grlieu') }}
),

final as (
    Select *
    from v_stg_grlieu
)

select * from final