{% snapshot export_organisation_snapshot %}

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
	organisation_mere,
	societe_mere,
	type_d_organisation,
	tel_intervenant,
	actif,
	mode_de_propagation_par_defaut,
	mode_de_propagation_force,
	date_de_debut,
	date_de_fin,
	adresse_postale,
	mode_de_propagation_nominal,
	mode_de_propagation_nominal_cible,
	mode_de_propagation_degrade,
	mode_de_propagation_degrade_cible,
	commmentaire
  from {{ source('rdd_export', 'export_organisation') }}

{% endsnapshot %}
    