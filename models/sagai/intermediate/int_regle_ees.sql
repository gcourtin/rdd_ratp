with net_regle_ees as (
    select * from {{ ref('net_regle_ees') }}
),

final as (
    select 
        regle_id,
        listagg(distinct ees_id,',') ees_id
    from net_regle_ees
    group by regle_id
)

select * from final