with source as (

    select * from {{ source('sagai_raw', 'organisation_reseau') }}

),

renamed as (

    select
        or_organisation_id,
        or_reseau_id,
        or_date_cre,
        or_utilisateur_cre

    from source

)

select * from renamed