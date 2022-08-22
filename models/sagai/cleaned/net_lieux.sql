with v_ext_lieu_si_immo as (
    select * from  {{ ref('ext_lieu_si_immo') }} 
 ),

 v_detail_grlieu as (
     select * from {{ ref('net_detail_grlieu') }}
 ),

 v_grlieu as (
     select * from {{ ref('net_grlieu') }}    
 ),

 v_acc_lieux as (
     select * from {{ ref('acc_lieux_A') }}
 ),

final as (
    Select 
        *
        from (
            select distinct
                substring(a.elo_id,1,3) ugcode,
                'RATP' as ugpere,
                'TRS'  as reseau_code,
                'Transverse' as reseau_libelle,
                'AUT' famille_code,
                'Autre' famille_libelle,
                NULL type_libelle,
                NULL commentaire,
                NULL com_insee,
                NULL cdpostal,
                NULL typvoie,
                NULL numvoie,
                NULL nomvoie,
                NULL bp,
                NULL cedex,
                NULL commune,
                NULL adr_comment,
                case
                    when substring(a.elo_id,2,2)<=20 then 'PARIS '||substring(a.elo_id,2,2) else 'DEPT '||substring(a.elo_id,2,2)
                end as libcrt,
                case
                    when substring(a.elo_id,2,2)<=20 then 'ARRONDISSEMENT PARIS '||substring(a.elo_id,2,2) else 'DEPARTEMENT '||substring(a.elo_id,2,2)
                end as liblng,
                'DEPT-ARRDT' type_elo,
                NULL log_date_crea,
                NULL log_date_modi,
                NULL codemars,
                NULL destina1_code,
                NULL destina1_libelle,
                NULL destina2_code,
                NULL destina2_lib,
                NULL cdcourrier,
                'DEPT-ARRDT' famelo_code,
                'DEPARTEMENT-ARRONDISSEMENT' famelo_lib,
                NULL codesite,
                NULL libelle_site,
                NULL resp_nom,
                NULL resp_prenom,
                NULL resp_cptematriculaire,
                NULL resp_tel,
                NULL resp_fax,
                NULL commentaire_site,
                NULL cl_affecta,
                NULL cl_occup,
                NULL x,
                NULL y,
                substring(a.elo_id,1,3) elo_id,
                a.ugpere as parent_elo_id
            from v_ext_lieu_si_immo a
            left join v_ext_lieu_si_immo b on substring(a.elo_id,1,3)= b.elo_id
            where length(a.elo_id)=6 and b.elo_id is null
            union all
            select 
                a.ugcode,
                substring(a.elo_id,1,3) ugpere,
                a.reseau_code,
                a.reseau_libelle,
                a.famille_code,
                a.famille_libelle,
                a.type_libelle,
                a.commentaire,
                a.com_insee,
                a.cdpostal,
                a.typvoie,
                a.numvoie,
                a.nomvoie,
                a.bp,
                a.cedex,
                a.commune,
                a.adr_comment,
                a.libcrt,
                a.liblng,
                a.type_elo,
                a.log_date_crea,
                a.log_date_modi,
                a.codemars,
                a.destina1_code,
                a.destina1_libelle,
                a.destina2_code,
                a.destina2_lib,
                a.cdcourrier,
                a.famelo_code,
                a.famelo_lib,
                a.codesite,
                a.libelle_site,
                a.resp_nom,
                a.resp_prenom,
                a.resp_cptematriculaire,
                a.resp_tel,
                a.resp_fax,
                a.commentaire_site,
                a.cl_affecta,
                a.cl_occup,
                a.x,
                a.y,
                a.elo_id,
                substring(a.elo_id,1,3) as parent_elo_id
            from v_ext_lieu_si_immo a
            left join v_ext_lieu_si_immo b on substring(a.elo_id,1,3)= b.elo_id
            where length(a.elo_id)=6 and b.elo_id is null
            union all
            select 
                a.ugcode,
                a.ugpere,
                a.reseau_code,
                a.reseau_libelle,
                a.famille_code,
                a.famille_libelle,
                a.type_libelle,
                a.commentaire,
                a.com_insee,
                a.cdpostal,
                a.typvoie,
                a.numvoie,
                a.nomvoie,
                a.bp,
                a.cedex,
                a.commune,
                a.adr_comment,
                a.libcrt,
                a.liblng,
                a.type_elo,
                a.log_date_crea,
                a.log_date_modi,
                a.codemars,
                a.destina1_code,
                a.destina1_libelle,
                a.destina2_code,
                a.destina2_lib,
                a.cdcourrier,
                a.famelo_code,
                a.famelo_lib,
                a.codesite,
                a.libelle_site,
                a.resp_nom,
                a.resp_prenom,
                a.resp_cptematriculaire,
                a.resp_tel,
                a.resp_fax,
                a.commentaire_site,
                a.cl_affecta,
                a.cl_occup,
                a.x,
                a.y,
                a.elo_id,
                a.parent_elo_id
            from v_ext_lieu_si_immo a
            left join v_ext_lieu_si_immo b on substring(a.elo_id,1,3)= b.elo_id
            where (length(a.elo_id)=6 and b.elo_id is not null) or length(a.elo_id)<>6
        )
        where elo_id not like '7%' and elo_id not like '199%' /* and 
              elo_id not in (
                            select al.elo_id from v_grlieu  gr
                            inner join v_detail_grlieu dg on dg.grlieu_id=gr.grlieu_id
                            inner join v_acc_lieux al on gr.elo_id=al.lieu_id
                            where dg.collection_id = 3
                            ) */
             and type_elo not like  'TERRAIN_S' and type_elo not like  'TERRAIN_L'
)

select * from final