with v_map_reseau_fonction_ees as (
    select * from {{ ref('map_reseau_fonction_ees') }}
),

final as (
    Select *
    from v_map_reseau_fonction_ees
    where fonction_merlin not like 'BUS%'
)

select * from final