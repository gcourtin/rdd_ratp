with export_lieux_snapshot as (
    select * from  {{ ref('export_lieux_snapshot') }}
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
	case when dbt_valid_to is null then true else false end actif
from
	export_lieux_snapshot
where coalesce (dbt_valid_to,dbt_valid_from)>=to_timestamp('{{ var("last_date_snap") }}')
qualify dense_rank() over (partition by eloid order by dbt_valid_from desc,nvl(dbt_valid_to,to_timestamp('2999-12-31 09:00')) desc) = 1
)

select * from final