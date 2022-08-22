{% snapshot export_lieux_snapshot %}

  {{
      config(
        target_database='rdd',
        target_schema='dev_99_snapshots',
        unique_key='code_ug',

        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True,
      )
  }}

  select 
    code_ug,
	eloid,
	code_ug_pere,
	reseau_code,
	reseau_libelle,
	code_famille_de_lieux,
	libelle_famille_de_lieux,
	type_libelle,
	commentaire,
	code_postal,
	type_de_voie,
	nemero_de_la_voie,
	nom_de_la_voie,
	boite_postale,
	cedex,
	commune,
	libelle_court_du_lieu,
	libelle_long_du_lieu,
	type_de_lieu,
	destina1_code,
	destina1_libelle,
	destina2_code,
	destina2_lib,
	famelo_code,
	famelo_lib
  from {{ source('rdd_export', 'export_lieux') }}

{% endsnapshot %}
    
