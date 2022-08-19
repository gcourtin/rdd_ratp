with v_stg_inter_station as (
    select * from {{ ref('stg_inter_station') }}
),

final as (
    Select *
    from v_stg_inter_station
)

select * from final