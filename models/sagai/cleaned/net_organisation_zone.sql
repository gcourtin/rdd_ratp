with v_organisation_zone as (
    select * from {{ ref('stg_organisation_zone') }}
),

v_organisation as (
    select * from {{ ref('net_organisation') }}
),

final as (
    Select 
         z.*
         from v_organisation_zone z
         inner join v_organisation o on z.oz_organisation_id=o.organisation_id
)

select * from final