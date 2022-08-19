with v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_ees as (
    select * from {{ ref('net_ees') }}
),

v_regle_ees as (
    select * from {{ ref('stg_regle_ees') }}
),

v_regle_lieu as (
    select * from {{ ref('stg_regle_lieu') }}
),

v_regle as (
    select * from {{ ref('stg_regle') }}   
),

final as (
    Select 
         r.* 
         from v_regle r
         inner join v_organisation o on o.organisation_id=r.organisation_id
         where r.regle_id in (
                             select distinct re.regle_id from v_regle_ees re
                             inner join v_ees e on re.ees_id=e.ees_id and re.regle_id=r.regle_id
                             inner join v_regle_lieu rl on rl.regle_id=r.regle_id
                             )
                and (r.regle_dat_fin >= current_date() or r.regle_dat_fin is null)

)

select * from final