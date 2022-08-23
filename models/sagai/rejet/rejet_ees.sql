with ees_detail as (
    select * from {{ ref('ees_detail') }}
),

v_ees as (
    select * from {{ ref('stg_ees') }}
),

final as (
    select ed.ees_id,
    case 
        when ed.ees_npu = 1 then 'EES rejeté de la reprise car à ne plus utiliser (NPU)'
        when ed.ees_rejete_fonction_Bus = 1 then 'EES rejeté de la reprise car uniquement lié à des fonctions BUS'
        when ed.avec_fonction = 0 then 'EES rejeté de la reprise car non lié a une fonction'
        else 'Raison inconnue'
    end as motif
    from ees_detail ed
    inner join v_ees e on e.ees_id=ed.ees_id
    where ed.ees_non_repris = 1
)

select * from final