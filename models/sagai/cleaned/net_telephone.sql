with v_stg_telephone as (
    select * from {{ ref('stg_telephone') }}
),

final as (
    Select *
    from v_stg_telephone
)

select * from final