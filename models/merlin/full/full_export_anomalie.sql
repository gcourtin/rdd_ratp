with export_anomalie as (
    select * from  {{ ref('export_anomalie') }}
),

final as (
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
    from
        export_anomalie
)

select * from final