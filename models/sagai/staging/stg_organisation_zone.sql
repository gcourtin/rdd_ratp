with source as (

    select * from {{ source('sagai_raw', 'organisation_zone') }}

),

renamed as (

    select
        oz_organisation_id,
        oz_zonegeo_id,
        oz_date_cre,
        oz_utilisateur_cre

    from source

)

select * from renamed