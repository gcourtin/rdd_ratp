with v_stg_detail_grlieu as (
    select * from {{ ref('stg_detail_grlieu') }}
),

final as (
    Select *
    from v_stg_detail_grlieu
)

select * from final