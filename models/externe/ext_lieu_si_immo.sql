with source as (

    select * from {{ source('externe_data', 'lieu_si_immo') }}

),

renamed as (

    select
        ugcode,
        ugpere,
        reseau_code,
        reseau_libelle,
        famille_code,
        famille_libelle,
        type_libelle,
        commentaire,
        com_insee,
        cdpostal,
        typvoie,
        numvoie,
        nomvoie,
        bp,
        cedex,
        commune,
        adr_comment,
        libcrt,
        liblng,
        type_elo,
        log_date_crea,
        log_date_modi,
        codemars,
        destina1_code,
        destina1_libelle,
        destina2_code,
        destina2_lib,
        cdcourrier,
        famelo_code,
        famelo_lib,
        codesite,
        libelle_site,
        resp_nom,
        resp_prenom,
        resp_cptematriculaire,
        resp_tel,
        resp_fax,
        commentaire_site,
        cl_affecta,
        cl_occup,
        x,
        y,
        replace(ugcode,'-') elo_id,
        replace(ugpere,'-') parent_elo_id

    from source

)

select * from renamed