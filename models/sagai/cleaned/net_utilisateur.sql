with v_stg_utilisateur as (
    select * from {{ ref('stg_utilisateur') }}
),

final as (
    Select *
    from v_stg_utilisateur
)

select * from final