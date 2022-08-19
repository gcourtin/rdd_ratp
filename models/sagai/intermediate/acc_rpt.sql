with v_rpt as (
    select elo_id from  {{ ref('stg_rpt') }} 
 ),

final as (
	select
		distinct *
	from
		(
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 1) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 3) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 4) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 6) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 8) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 9) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 10) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 11) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 12) as lieu_id
			from
				v_rpt rpt
			union all
			select
				rpt.elo_id ,
				substring(rpt.elo_id, 1, 17) as lieu_id
			from
				v_rpt rpt
		)
)

select * from final