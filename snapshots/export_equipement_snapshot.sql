{% snapshot export_equipement_snapshot %}

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
    nom,
    actif,
    equipement_asservi,
    code_gmao,
    responsable,
    lieu,
    ees,
    organisation,
    xpeage,
    source,
    description,
    statut,
    regle,
    code_mire
  from {{ source('rdd_export', 'export_equipement') }}

{% endsnapshot %}