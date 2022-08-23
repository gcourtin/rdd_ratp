with organisation_detail as (
    select * from {{ ref('organisation_detail') }}
),

v_organisation as (
    select * from {{ ref('stg_organisation') }}
),

final as (
    select od.organisation_id,
    case 
        when od.npu = 1 then 'Organisation comprenant les mots suivant:  Z-REFOR, NPU, METEO et GUEST'
        else 'Raison inconnue'
    end as motif
    from organisation_detail od
    inner join v_organisation o on o.organisation_id=od.organisation_id
    where od.org_non_repris = 1
)

select * from final