with v_stg_zonegeo as (
    select * from {{ ref('stg_zonegeo') }}
),

final as (
    Select *
    from v_stg_zonegeo
)

select * from final