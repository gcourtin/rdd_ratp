with net_mots_clefs as (
    select * from {{ ref('net_mots_clefs') }}
),

final as (
    select 
        regle_id,
        listagg(distinct mot_clef,',') mot_clef
    from net_mots_clefs
    where length(replace(regle_id,' '))<>1
    group by regle_id
)

select * from final