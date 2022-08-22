{% snapshot export_role_fonction_snapshot %}

  {{
      config(
        target_database='rdd',
        target_schema='dev_99_snapshots',
        unique_key='nom',

        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True,
      )
  }}

  select 
	nom,
	description
  from {{ source('rdd_export', 'export_role_fonction') }}

{% endsnapshot %}