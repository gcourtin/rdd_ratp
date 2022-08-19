with v_grlieu as (
    select * from {{ ref('int_grlieu') }}
),

v_detail_grlieu as (
    select * from {{ ref('net_detail_grlieu') }}
),

v_net_lieux as (
    select * from {{ ref('net_lieux') }}
),

final as (
    Select
         distinct grlieu_id, collection_id, espace, elo_id,
         case 
         when regexp_count(espace,',')>0 
             then 
                 case 
                     when regexp_count(espace,'Bâtiments')=0 and regexp_count(espace,'BUS')=0 
                         then 'Espaces FERRE' 
                         else 'Multimode' 
                     end
             else
                espace
         end as type_espace 
        from (
            select
                 gr.grlieu_id,
                 dg.collection_id,
                 listagg(distinct gr.elo_id,',') elo_id,
                 listagg(distinct 
                    case
                        when substring(gr.elo_id,1,3) in ('136','137','138','146','148','150') then 'Espaces RER'
                        when substring(gr.elo_id,1,3) in ('175','178','180','181','182','183','192','193','194') then 'Espaces TRAM'
                        when substring(gr.elo_id,1,3)<='101' and substring(gr.elo_id,1,3)<='119' then 'Espaces METRO'
                        when substring(gr.elo_id,1,3) = '199' or substring(gr.elo_id,1,1)='7' then 'Espaces BUS'
                        when substring(gr.elo_id,1,1)='2' then 'Bâtiments'
                        -- when dg.collection_id='1' then 'Espaces RER'
                        -- when dg.collection_id='2' then 'Espaces METRO'
                        -- when dg.collection_id='3' then 'Espaces BUS'
                        -- when dg.collection_id='4' then 'Bâtiments'
                        else 'Multimode'
                    end
                ,',') espace
            from v_grlieu gr
            inner join v_detail_grlieu dg on dg.grlieu_id = gr.grlieu_id
            group by gr.grlieu_id,dg.collection_id
            )
)

select * from final