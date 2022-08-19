with v_stg_profil as (
    select * from {{ ref('stg_profil') }}
),

final as (
    Select *
    from v_stg_profil
)

select * from final