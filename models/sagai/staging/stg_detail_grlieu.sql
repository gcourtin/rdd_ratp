with source as (

    select * from {{ source('sagai_raw', 'detail_grlieu') }}

),

renamed as (

    select
        grlieu_id,
        collection_id

    from source

)

select * from renamed