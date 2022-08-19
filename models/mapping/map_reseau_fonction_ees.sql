with source as (

    select * from {{ source('mapping_data', 'mapping_reseau_fonction_ees') }}

),

renamed as (

    select distinct
        fonction_merlin,
        fonction_id,
        ees_id,
        ees_libl,
        lower(replace(mts,' ')) mts,
        lower(replace(rer,' ')) rer,
        lower(replace(sem,' ')) sem,
        lower(replace(sur,' ')) sur,
        lower(replace(mrf,' ')) mrf,
        lower(replace(tram,' ')) tram,
        lower(replace(bus,' ')) bus,
        lower(replace(rre,' ')) rre

    from source

)

select * from renamed