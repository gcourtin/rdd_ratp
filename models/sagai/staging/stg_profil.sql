with source as (

    select * from {{ source('sagai_raw', 'profil') }}

),

renamed as (

    select
        profil_id,
        profil_groupe,
        profil_pass,
        profil_desc

    from source

)

select * from renamed