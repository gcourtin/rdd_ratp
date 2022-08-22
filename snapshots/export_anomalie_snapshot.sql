{% snapshot export_anomalie_snapshot %}

  {{
      config(
        target_database='rdd',
        target_schema='dev_99_snapshots',
        unique_key='identifiant',

        strategy='ckeck',
        check_cols='all',
      )
  }}

  select 
    identifiant,
    libelle,
    description,
    type_d_equipement,
    actif,
    niveau_d_urgence,
    etat_de_l_equipement,
    situation_inacceptable,
    critere_de_nettete,
    piece_jointe_obligatoire,
    verification_exploitant,
    sensible
  from {{ source('rdd_export', 'export_anomalie') }}

{% endsnapshot %}