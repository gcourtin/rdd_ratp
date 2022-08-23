with v_stg_zonegeo as (
    select * from {{ ref('net_zonegeo') }}
),

v_int_grlieu_espace as (
    select * from {{ ref('int_grlieu_espace') }}
),

final as (
    Select
        z.zonegeo_id old_zonegeo_id,
        case 
            when z.zonegeo_id = z2.grlieu_id 
            then z.zonegeo_id||'_Z' 
            else z.zonegeo_id 
        end zonegeo_id,
        z.grlieu_id,
        z.collection_id 
        from v_stg_zonegeo z
        inner join v_int_grlieu_espace le on z.grlieu_id=le.grlieu_id
        left join v_stg_zonegeo z2 on z.zonegeo_id = z2.grlieu_id
)

select * from final