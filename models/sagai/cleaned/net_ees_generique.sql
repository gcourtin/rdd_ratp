with v_stg_ees_generique as (
    select * from {{ ref('stg_ees_generique') }}
),

final as (
    Select *
    from v_stg_ees_generique
)

select * from final