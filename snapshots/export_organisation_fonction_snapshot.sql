{% snapshot export_organisation_fonction_snapshot %}

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
	fonction_merlin
  from {{ source('rdd_export', 'export_organisation_fonction') }}

{% endsnapshot %}
    