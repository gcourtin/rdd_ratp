with v_stg_zonegeo as (
    select * from {{ ref('net_zonegeo') }}
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
        left join v_stg_zonegeo z2 on z.zonegeo_id = z2.grlieu_id
)

select * from final