with source as (

    select * from {{ source('mdw_export', 'export_groupe_de_lieux') }}

),

renamed as (

    select
        identifiant,
        type_espace,
        parent,
        elo_id,
        actif,
        description

    from source

)

select * from renamed