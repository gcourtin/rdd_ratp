with source as (

    select * from {{ source('mdw_export', 'export_organisation_groupe_de_lieux') }}

),

renamed as (

    select
        identifiant,
        nom

    from source

)

select * from renamed