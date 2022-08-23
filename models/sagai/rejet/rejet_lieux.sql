with ext_lieu_si_immo as (
    select * from  {{ ref('ext_lieu_si_immo') }}
),

export_lieux as (
    select * from  {{ ref('export_lieux') }}
),

final as (
    select a.elo_id
     case 
        when a.type_elo like 'TERRAIN%' then 'Rejet Lieu '||a.type_elo
        when a.elo_id like '199%' or a.elo_id like '7%' then 'Rejet Lieu BUS'
        else a.type_elo
     end motif
    from ext_lieu_si_immo a
    left join export_lieux b on a.elo_id =b.eloid 
    where b.eloid is null 
)

select * from final