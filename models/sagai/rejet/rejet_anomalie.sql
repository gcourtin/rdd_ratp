with anomalie_detail as (
    select * from {{ ref('anomalie_detail') }}
),

v_anomalie as (
    select * from {{ ref('stg_anomalie') }}
),

rejet_ees as (
    select * from {{ ref('rejet_ees') }}
),

final as (
    select ad.anomalie_id,a.ees_id,
    case 
        when ad.ees_id_null = 1 then 'Anomalie non reprise car anomalie sans EES'
        when ad.ees_present = 0 then 'Anomalie non reprise car EES inconnu'
        when ad.ees_non_repris = 1 then 'Anomalie non reprise car '||re.motif
        else 'Raison inconnue'
    end as motif
    from anomalie_detail ad
    inner join v_anomalie a on a.anomalie_id=ad.anomalie_id
    left join rejet_ees re on re.ees_id=a.ees_id
    where ad.ano_non_reprise = 1
)

select * from final