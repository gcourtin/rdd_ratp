{% snapshot export_last_run_date_snapshot %}

  {{
      config(
        target_database='rdd',
        target_schema='dev_99_snapshots',
        unique_key='id',

        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True,
      )
  }}

  select 
    id,
    lastrun
  from {{ source('rdd_export', 'export_last_run_date') }}

{% endsnapshot %}