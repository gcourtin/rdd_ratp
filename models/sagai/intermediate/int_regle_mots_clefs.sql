with net_mots_clefs as (
    select * from {{ ref('net_mots_clefs') }}
),

v_export_equipement as (
    select * from {{ ref('export_equipement') }}
),

final as (
    select 
        a.regle_id,
        listagg(distinct a.mot_clef,',') mot_clef
    from net_mots_clefs a
    inner join v_export_equipement b on a.mot_clef = b.identifiant
    where length(replace(regle_id,' '))<>1
    group by a.regle_id
)

select * from final