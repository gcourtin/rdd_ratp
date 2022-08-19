with v_net_lieux as (
    select * from  {{ ref('net_lieux') }} 
 ),

 final as (
     select
        ugcode as code_ug,
        elo_id as eloid,
        ugpere as code_ug_pere,
        reseau_code as reseau_code,
        reseau_libelle as reseau_libelle,
        famille_code as code_famille_de_lieux,
        famille_libelle as libelle_famille_de_lieux,
        type_libelle as type_libelle,
        commentaire as commentaire,
        cdpostal as code_postal,
        typvoie as type_de_voie,
        numvoie as nemero_de_la_voie,
        nomvoie as nom_de_la_voie,
        bp as boite_postale,
        cedex as cedex,
        commune as commune,
        libcrt as libelle_court_du_lieu,
        liblng as libelle_long_du_lieu,
        type_elo as type_de_lieu,
        destina1_code as destina1_code,
        destina1_libelle as destina1_libelle,
        destina2_code as destina2_code,
        destina2_lib as destina2_lib,
        famelo_code as famelo_code,
        famelo_lib as famelo_lib
    from v_net_lieux
 )

 select * from final