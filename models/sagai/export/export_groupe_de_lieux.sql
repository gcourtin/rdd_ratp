with v_int_zonegeo as (
    select * from {{ ref('int_zonegeo') }}
),

v_int_grlieu_espace as (
    select * from {{ ref('int_grlieu_espace') }}
),

final as (
    Select 
		 identifiant,
		 case 
			when regexp_count(type_espace,',')>0 
			then 
				case when regexp_count(type_espace,'Bâtiment')=0 and regexp_count(type_espace,'BUS')=0 and regexp_count(type_espace,'Multimode')=0 then 'Espaces FERRE' else 'Multimode' end
			else
				type_espace
		 end as type_espace,
		 '' as parent,
		 '' as elo_id,
		 true actif,
		 '' as description 
	from (
		 select parent as identifiant, listagg(distinct type_espace,',') as type_espace
		 from (
			select z.grlieu_id as identifiant, z.zonegeo_id parent,v.espace as type_espace
			from v_int_zonegeo z
			inner join v_int_grlieu_espace v on z.grlieu_id =v.grlieu_id
			)
		 group by parent)
	union all
	select 
		 a.identifiant, 
		 b.type_espace,
		 a.parent,
		 b.elo_id,
		 true actif,
		 '' as description
	from
		 (
			select z.grlieu_id identifiant,listagg(distinct z.zonegeo_id,',') parent
			from v_int_zonegeo z group by z.grlieu_id
		 ) a
		 inner join v_int_grlieu_espace b on a.identifiant=b.grlieu_id
)

select * from final