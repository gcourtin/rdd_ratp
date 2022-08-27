with source as (

    select * from {{ source('mdw_export', 'export_anomalie') }}

),

renamed as (

    select
        identifiant,
        libelle,
        description,
        type_d_equipement,
        actif,
        niveau_d_urgence,
        etat_de_l_equipement,
        situation_inacceptable,
        critere_de_nettete,
        piece_jointe_obligatoire,
        verifications_exploitant,
        sensible

    from source

)

select * from renamed