with v_stg_inventaire_auxiliaire as (
    select * from {{ ref('stg_inventaire_auxiliaire') }}
),

final as (
    Select *
    from v_stg_inventaire_auxiliaire
)

select * from final