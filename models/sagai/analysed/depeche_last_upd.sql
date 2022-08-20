with v_depeche as (
    select * from {{ ref('stg_depeche') }} 
),

v_bloc as (
    select * from {{ ref('stg_bloc') }} 
),

v_evenement as (
    select * from {{ ref('stg_evenement') }} 
),

v_emission as (
    select * from {{ ref('stg_emission') }} 
),

final as (
    select 
        de.depeche_date,
        de.depeche_num,
        de.depeche_statut,
        max(b.bloc_date_cre) max_bloc_date,
        max(e.evt_date_cre) max_evt_date,
        max(em.emission_date) max_emi_date,
        greatest(
            de.depeche_date,
            max(nvl(b.bloc_date_cre,to_date('1900-01-01'))),
            max(nvl(e.evt_date_cre,to_date('1900-01-01'))),
            max(nvl(em.emission_date,to_date('1900-01-01')))
            ) as last_action_date
    from v_depeche de
    left join v_bloc b on (de.depeche_date = b.depeche_date and de.depeche_num = b.depeche_num)
    left join v_evenement e on (b.depeche_date = e.depeche_date and b.depeche_num = e.depeche_num and b.bloc_num=e.bloc_num)
    left join v_emission em on (em.depeche_date = e.depeche_date and em.depeche_num = e.depeche_num and em.bloc_num=e.bloc_num and em.evt_num=e.evt_num)
    group by de.depeche_date,de.depeche_num,de.depeche_statut
)

select * from final