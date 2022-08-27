with source as (

    select * from {{ source('mdw_export', 'export_ees') }}

),

renamed as (

    select
        id_sagai,
        libelle,
        sous_type,
        url_unc,
        inventaire_autorise,
        actif,
        commentaire,
        reseaux_fonctions,
        station,
        elo_id,
        x_peage

    from source

)

select * from renamed