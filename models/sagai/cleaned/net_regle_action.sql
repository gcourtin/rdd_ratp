with v_regle as (
    select * from {{ ref('net_regle') }}
 ),

v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_regle_action as (
        Select * from  {{ ref('stg_regle_action') }}
),

final as (
    Select 
         ra.* 
         from  v_regle_action ra
         inner join v_regle r on ra.regle_id=r.regle_id
         inner join v_organisation a on a.organisation_id=ra.organisation_id
)

select * from final