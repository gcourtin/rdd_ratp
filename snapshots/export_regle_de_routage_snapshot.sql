{% snapshot export_regle_de_routage_snapshot %}

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
	libelle,
	description,
	actif,
	date_debut,
	date_fin,
	plage_horaire,
	heure_de_debut,
	heure_de_fin,
	poids,
	lieux,
	ees,
	equipement,
	anomalie,
	intervenant_principal,
	intervenant_pour_action,
	intervenant_en_copie
  from {{ source('rdd_export', 'export_regle_de_routage') }}

{% endsnapshot %}