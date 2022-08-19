with v_organisation_reseau as (
    select * from {{ ref('stg_organisation_reseau') }}
),

v_organisation as (
    select * from {{ ref('net_organisation') }}
),

final as (
    Select 
        r.* 
        from v_organisation_reseau r
        inner join v_organisation o on r.or_organisation_id=o.organisation_id
)

select * from final