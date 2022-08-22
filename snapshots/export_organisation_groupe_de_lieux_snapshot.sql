{% snapshot export_organisation_groupe_de_lieux_snapshot %}

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
     nom
  from {{ source('rdd_export', 'export_organisation_groupe_de_lieux') }}

{% endsnapshot %}