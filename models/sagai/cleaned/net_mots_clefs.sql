with v_net_ees as (
    select * from {{ ref('net_ees') }}
),

v_net_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_net_regle as (
    select * from {{ ref('net_regle') }}
),

v_net_lieux as (
    select * from {{ ref('net_lieux') }}
),

v_mots_clefs as (
    select * from {{ ref('stg_mots_clefs') }}
),

final as (
    Select 
        m.* 
        from v_mots_clefs m
        left join v_net_ees e on e.ees_id=m.ees_id
        left join v_net_organisation o on o.organisation_id = m.organisation_id
        left join v_net_regle r on r.regle_id=m.regle_id
        left join v_net_lieux l on l.elo_id=m.elo_id
        where (m.ees_id is null or e.ees_id is not null)
        and   (m.organisation_id is null or o.organisation_id is not null)
        and   (m.regle_id is null or r.regle_id is not null or length(replace(m.regle_id,' ')) = 1)
        and   (m.refpat112_id is null or l.elo_id is not null)
)

select * from final