{% snapshot export_groupe_de_lieux_snapshot %}

  {{
      config(
        target_database='rdd',
        target_schema='dev_99_snapshots',
        unique_key='identifiant',

        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True,
      )
  }}

  select 
    identifiant,
    type_espace,
    parent,
    elo_id,
    actif,
    description
  from {{ source('rdd_export', 'export_groupe_de_lieux') }}

{% endsnapshot %}