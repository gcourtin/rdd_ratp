with v_regle as (
    select * from {{ ref('net_regle') }}
 ),

 v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_regle_copie as (
     select * from  {{ ref('stg_regle_copie') }}
 ),

final as (
    Select 
         rc.* 
         from  v_regle_copie rc
         inner join v_regle r on rc.regle_id=r.regle_id
         inner join v_organisation a on a.organisation_id=rc.organisation_id
)

select * from final