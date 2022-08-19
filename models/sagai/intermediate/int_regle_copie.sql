with v_net_regle_copie as (
    select * from {{ ref('net_regle_copie') }}
 ),

final as (
     select 
        regle_id,
        listagg(distinct organisation_id,',') organisation_id
     from v_net_regle_copie
     group by regle_id
)

select * from final