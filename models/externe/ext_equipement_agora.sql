with source as (

    select * from {{ source('externe_data', 'equipement_agora') }}

),

renamed as (

    select
        code_bm,
        description_bm,
        code_refpat,
        descriptif_ees,
        numero_ees,
        ees_id,
        action,
        statut,
        x_peage,
        service_maint,
        libelle_service_maint,
        gmao

    from source

)

select * from renamed