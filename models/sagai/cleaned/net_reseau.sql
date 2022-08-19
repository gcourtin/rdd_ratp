with v_stg_reseau as (
    select * from {{ ref('stg_reseau') }}
),

final as (
    Select *
    from v_stg_reseau
    where reseau_id not in (3,11)
)

select * from final