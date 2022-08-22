{% snapshot export_ees_snapshot %}

  {{
      config(
        target_database='rdd',
        target_schema='dev_99_snapshots',
        unique_key='id_sagai',

        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True,
      )
  }}

  select 
    id_sagai,
    libelle,
    sous_type,
    url_unc,
    inventaire_autorise
    actif,
    commentaire,
    reseaux_fonctions,
    station,
    elo_id,
    x_peage
  from {{ source('rdd_export', 'export_ees') }}

{% endsnapshot %}