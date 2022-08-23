with export_role_fonction as (
    select * from  {{ ref('export_role_fonction') }}
),

final as (
    select 
        nom,
        description,
        true actif
    from
        export_role_fonction
)

select * from final