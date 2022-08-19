with source as (

    select * from {{ source('mapping_data', 'mapping_fonction_merlin') }}

),

renamed as (

    select
        fonction_libc_sagai,
        fonction_libc_merlin

    from source

)

select * from renamed