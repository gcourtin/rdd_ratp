with source as (

    select * from {{ source('sagai_raw', 'rpt') }}

),

renamed as (

    select
        elo_id,
        elo_libc,
        elo_libl,
        famelo_code,
        adresse_elo_id,
        rpt_gerant_elo,
        rpt_utilisation_elo,
        rpt_info_tel_elo,
        rpt_info_cle_elo,
        rpt_info_3_elo,
        rpt_reseau_elo_id,
        rpt_long_elo_id,
        rpt_fam_mire,
        elo_typ,
        elo_mars,
        rpt_surface_elo,
        rpt_zone_critique,
        rpt_type_arret,
        rpt_num_gipa

    from source

)

select * from renamed