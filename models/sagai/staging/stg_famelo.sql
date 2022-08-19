with source as (

    select * from {{ source('sagai_raw', 'famelo') }}

),

renamed as (

    select
        famelo_code,
        famelo_libl

    from source

)

select * from renamed