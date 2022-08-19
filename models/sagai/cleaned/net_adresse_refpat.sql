with v_rpt as (
    select * from  {{ ref('net_rpt') }}
),

v_adresse_refpat as (
    select * from {{ ref('stg_adresse_refpat') }}
),

final as (
    Select 
        *
        from v_adresse_refpat
        where adresse_elo_id in (select distinct adresse_elo_id from v_rpt)
)

select * from final