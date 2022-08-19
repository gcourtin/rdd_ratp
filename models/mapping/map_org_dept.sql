with source as (

    select * from {{ source('mapping_data', 'mapping_org_dept') }}

),

renamed as (

    select
        organisation_id,
        organisation_nom,
        organisation_id_pere,
        organisation_nom_pere,
        departement,
        organisation_type,
        commentaire,
        non_repris_merlin

    from source

)

select * from renamed