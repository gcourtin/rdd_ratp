with export_lieux as (
    select * from  {{ ref('export_lieux') }}
),

final as (
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
	famelo_lib,
	true actif
from
	export_lieux
)

select * from final