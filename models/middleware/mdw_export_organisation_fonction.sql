with source as (

    select * from {{ source('mdw_export', 'export_organisation_fonction') }}

),

renamed as (

    select
        identifiant,
        fonction_merlin

    from source

)

select * from renamed