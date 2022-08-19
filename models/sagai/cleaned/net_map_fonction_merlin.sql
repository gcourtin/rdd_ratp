with v_map_fonction_merlin as (
    select * from {{ ref('map_fonction_merlin') }}
),

final as (
    select * from v_map_fonction_merlin
    where fonction_libc_merlin not like 'BUS%'
)

select * from final