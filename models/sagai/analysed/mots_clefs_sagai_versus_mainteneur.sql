with v_ees as (
    select * from {{ ref('stg_ees') }} 
),

v_regle_ees as (
    select * from {{ ref('stg_regle_ees') }} 
),

v_regle as (
    select * from {{ ref('stg_regle') }} 
),

v_regle_action as (
    select * from {{ ref('stg_regle_action') }} 
),

v_regle_copie as (
    select * from {{ ref('stg_regle_copie') }} 
),

v_regle_ano as (
    select * from {{ ref('stg_regle_ano') }} 
),

v_map_org_dept as (
    select * from {{ ref('map_org_dept') }} 
),

v_fonction_ees as (
    select * from {{ ref('stg_fonction_ees') }} 
),

v_reseau_fonction as (
    select * from {{ ref('stg_reseau_fonction') }} 
),

v_reseau as (
    select * from {{ ref('stg_reseau') }} 
),

v_grlieu as (
    select * from {{ ref('stg_grlieu') }} 
),

v_detail_grlieu as (
    select * from {{ ref('stg_detail_grlieu') }} 
),

v_lieu as (
    select * from {{ ref('acc_rpt') }}
),

v_ext_equipement_agora as (
    select * from {{ ref('ext_equipement_agora') }} 
),

v_mots_clefs as (
    select * from {{ ref('stg_mots_clefs') }} 
),

v_bloc as (
    select * from {{ ref('stg_bloc') }}     
),
 
mc_dept as (
	select mot_clef
		,listagg(departement, ',') within group (order by rn desc) dept
	from (
		select distinct m1.mot_clef
			,m1.ees_id
			,m1.elo_id
			,o.departement
			,count(b.depeche_num) over (partition by m1.mot_clef,m1.ees_id,m1.elo_id,o.departement order by m1.mot_clef,o.departement) rn
		from v_bloc b
		inner join v_map_org_dept o on o.organisation_id = b.bloc_org_id
		inner join v_mots_clefs m1 on (
				b.bloc_ees_id = m1.ees_id
				and m1.num_equip = b.bloc_num_equip
				and m1.elo_id like b.bloc_lieu_id || '%'
				and (
					m1.sous_elo = b.bloc_code_mire
					or b.bloc_code_mire is null
					)
				)
		)
	group by mot_clef
),

mc_agora
as (
	select m.mot_clef
		,m.code_gmao
		,a.code_bm
		,m.ees_id
		,a.ees_id ees_id_agora
		,(m.ees_id = a.ees_id) check_ees
	from v_mots_clefs m
	inner join v_ext_equipement_agora a on a.code_bm = m.code_gmao
	
	union
	
	select m.mot_clef
		,m.code_gmao
		,a.code_bm
		,m.ees_id
		,a.ees_id _agora
		,(m.ees_id = a.ees_id) check_ees
	from v_mots_clefs m
	inner join v_ext_equipement_agora a on a.code_bm = m.mot_clef
),

final as (
    select c.mot_clef
        ,num_equip
        ,c.code_gmao
        ,mots_clefs_gmao
        ,elo_id
        ,c.ees_id
        ,organisation_id
        ,org
        ,departement
        ,organisation_type
        ,contains (reseau_libc,'X - PEAGES') x_peages
        ,a.dept
        ,aa.check_ees check_agora
        ,contains (orgregleees,'M2E') orga_m2e
        ,contains (orgregleees,'GDI') orga_gdi
        ,contains (orgregleees,'VOIE') orga_voie
        ,(c.mot_clef like 'SAGAI%') SAGAI
        ,(c.mot_clef like 'ARES%') ARES
        ,(mots_clefs_gmao like '%GDI%') gmao_gdi
        ,(mots_clefs_gmao like '%M2E%' or upper(mots_clefs_gmao) = 'GMAO') gmao_m2e
    from (
        select m.mot_clef
            ,m.inventaire_aux_id
            ,m.num_equip
            ,m.code_gmao
            ,m.mots_clefs_gmao
            ,m.ees_id
            ,m.elo_id
            ,m.sous_elo code_mire
            ,m.organisation_id
            ,split_part(m.organisation_id, '/', 1) as org
            ,mo.departement
            ,mo.organisation_type
            ,listagg(distinct rr.reseau_libc, ',') reseau_libc
            ,listagg(distinct split_part(m.organisation_id, '/', 1), ',') orgmc
            ,listagg(distinct split_part(r.organisation_id, '/', 1), ',') orgregleees
            ,listagg(distinct split_part(ra.organisation_id, '/', 1), ',') orgraction
            ,listagg(distinct split_part(rc.organisation_id, '/', 1), ',') orgrcopie
            ,listagg(distinct split_part(ra1.organisation_id, '/', 1), ',') orgregleactionmc
        from v_mots_clefs m
        inner join v_ees e on m.ees_id = e.ees_id
        left join v_regle_ees re on m.ees_id = re.ees_id
        left join v_regle r on re.regle_id = r.regle_id
        left join v_regle_action ra on r.regle_id = ra.regle_id
        left join v_regle_copie rc on rc.regle_id = r.regle_id
        left join v_regle r1 on m.regle_id = r1.regle_id
        left join v_regle_action ra1 on r1.regle_id = ra1.regle_id
        left join v_map_org_dept mo on m.organisation_id = mo.organisation_id
        left join v_fonction_ees fe on m.ees_id = fe.ees_id
        left join v_reseau_fonction rf on rf.fonction_id = fe.fonction_id
        left join v_reseau rr on rf.reseau_id = rr.reseau_id
        where m.elo_id not like '7%'
            and m.elo_id not like '199%'
            and m.elo_id not in (
                select al.elo_id
                from v_grlieu gr
                inner join v_detail_grlieu dg on dg.grlieu_id = gr.grlieu_id
                inner join v_lieu al on gr.elo_id = al.lieu_id
                where dg.collection_id = 3
                )
            and upper(e.ees_libl) not like '%NE PLUS UTIL%'
            and e.ees_libl not like '%NPU%'
            and e.ees_libl not like '*%'
            and m.ees_id is not null
            and m.elo_id is not null
        group by m.mot_clef
            ,m.inventaire_aux_id
            ,m.num_equip
            ,m.code_gmao
            ,m.mots_clefs_gmao
            ,m.ees_id
            ,m.elo_id
            ,m.sous_elo
            ,m.organisation_id
            ,mo.departement
            ,mo.organisation_type
        ) c
    left join mc_dept a on a.mot_clef = c.mot_clef
    left join mc_agora aa on c.mot_clef = aa.mot_clef
)

select * from final