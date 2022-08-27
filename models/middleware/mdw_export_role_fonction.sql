with source as (

    select * from {{ source('mdw_export', 'export_role_fonction') }}

),

renamed as (

    select
        nom,
        description

    from source

)

select * from renamed