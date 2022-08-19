with source as (

    select * from {{ source('sagai_raw', 'emission') }}

),

renamed as (

    select
        depeche_date,
        depeche_num,
        bloc_num,
        evt_num,
        emission_num,
        emission_org_id,
        emission_type_trans,
        emission_type_bloc,
        emission_type_gmao,
        emission_copie,
        emission_date,
        emission_etat,
        emission_erreur

    from source

)

select * from renamed