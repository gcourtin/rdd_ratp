with net_regle_ano as (
    select * from {{ ref('net_regle_ano') }}
),

final as (
    select 
        regle_id,
        listagg(distinct anomalie_id,',') anomalie_id
    from net_regle_ano
    group by regle_id
)

select * from final