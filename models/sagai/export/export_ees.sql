with v_net_ees as (
    select * from {{ ref('net_ees') }}
),

v_fonction_merlin as (
    select * from {{ ref('int_fonction_merlin') }}
),

v_ees_generic as (
    select * from {{ ref('stg_ees_generique') }}
),

v_grlieu_merlin as (
            select distinct * from (
                select identifiant as grlieu_id,z.value as elo_id from (
                    select * from {{ ref('export_groupe_de_lieux') }} where elo_id is not null and elo_id <> ''), table(split_to_table(elo_id,',')
                ) z) order by grlieu_id,elo_id
),

v_net_mots_clefs as (
    select * from {{ ref('net_mots_clefs') }}
),

v_int_ees_xpeage as (
    select * from {{ ref('int_ees_xpeage') }}
),

v_net_map_fonction_merlin as (
    select * from {{ ref('net_map_fonction_merlin') }}
),

final as (
    Select 
        id_sagai,
        libelle,
        sous_type,
        URL_UNC,
        inventaire_autorise,
        actif,
        commentaire,
        listagg(distinct fonction_libc,',') as reseaux_fonctions,
        station,
        listagg(distinct elo_id,',') as elo_id,
        x_peage
     from (
         select distinct
            e.ees_id as ID_SAGAI,
            eg.grlieu_id,
            e.ees_libl as libelle,
            case 
                when eg.ees_id is null and m.ees_id is not null then 'Non générique' 
                when eg.ees_id is not null and m.ees_id is null then 'Générique par lieu'  
                when eg.ees_id is not null and m.ees_id is not null then 'Non générique, Générique par lieu' 
                else 'Non générique'
            end as sous_type,
            e.ees_url as URL_UNC,
            null as inventaire_autorise,
            true as actif,
            null as commentaire,
            trim(coalesce(mf.fonction_libc_merlin,f.fonction_libc)) fonction_libc,
            null as station,
            g.elo_id  as elo_id,
            x.x_peage
         from v_net_ees e
         left join v_ees_generic eg on eg.ees_id=e.ees_id 
         left join v_fonction_merlin f on e.ees_id =f.ees_id
         left join v_grlieu_merlin g on eg.grlieu_id = g.grlieu_id
         left join v_net_mots_clefs m on m.ees_id =e.ees_id
         left join v_int_ees_xpeage x on e.ees_id=x.ees_id
         left join v_net_map_fonction_merlin mf on trim(f.fonction_libc) = trim(mf.fonction_libc_sagai)  
         )
    group by id_sagai,libelle,sous_type,URL_UNC,inventaire_autorise,actif,commentaire,station,x_peage
    having reseaux_fonctions is not null and reseaux_fonctions <> ''
)

select * from final
