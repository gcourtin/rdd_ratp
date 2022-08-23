with export_ees as (
    select * from  {{ ref('export_ees_snapshot') }}
),

final as (
    select
        id_sagai,
        libelle,
        sous_type,
        url_unc,
        actif,
        commentaire,
        reseaux_fonctions,
        station,
        elo_id,
        x_peage
    from
        export_ees
)

select * from final