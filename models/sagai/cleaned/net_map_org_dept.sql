with v_map_org_dept as (
    select * from {{ ref('map_org_dept') }}
),

final as (
    Select *
    from v_map_org_dept
)

select * from final