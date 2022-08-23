with regles_detail as (
    select * from {{ ref('regles_detail') }}
),

final as (
    select rd.regle_id,
    case 
        when ees_inex = 1 then 'Règle rattaché à un ees inexistant'
        when ees_npu = 1 then 'Règle rattaché à un ees à ne plus utiliser'
        when lieu_bus = 1 then 'Règle associé à des lieux BUS'
        when ees_rejete = 1 then 'Règle associé à un ees rejeté'
        when lieu_inex = 1 then 'Règle associé à des lieux inexistant'
        when org_inex = 1 then 'Règle pour organisation inconue'
        when dat_fin = 1 then 'Règle dont la date de fin est dépassée'
        else 'Raison inconnue'
    end as motif
    from regles_detail rd
    where rd.regle_non_reprise = 1
)

select * from final