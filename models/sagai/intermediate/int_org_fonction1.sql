with v_map_reseau_fonction_ees as (
    select * from {{ ref('net_map_reseau_fonction_ees') }}
),

final as (
    Select
         distinct b.new_fonction_merlin as fonction_merlin, c.departement_id_fonction
         from v_map_reseau_fonction_ees a 
         inner join
        (
            select fonction_merlin, fonction_id, ees_id, ees_libl,'MTS'||fonction_id as departement_id_fonction,'MTS' as dept from v_map_reseau_fonction_ees where lower(mts)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'RER'||fonction_id as departement_id_fonction,'RER' as dept from v_map_reseau_fonction_ees where lower(rer)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'SEM'||fonction_id as departement_id_fonction,'SEM' as dept from v_map_reseau_fonction_ees where lower(sem)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'SUR'||fonction_id as departement_id_fonction,'SUR' as dept from v_map_reseau_fonction_ees where lower(sur)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'MRF'||fonction_id as departement_id_fonction,'MRF' as dept from v_map_reseau_fonction_ees where lower(mrf)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'TRAM'||fonction_id as departement_id_fonction,'TRAM' as dept from v_map_reseau_fonction_ees where lower(tram)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'BUS'||fonction_id as departement_id_fonction,'BUS' as dept from v_map_reseau_fonction_ees where lower(bus)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'RRE'||fonction_id as departement_id_fonction,'RRE' as dept from v_map_reseau_fonction_ees where lower(rre)='x' and fonction_id is not null
            ) c on a.fonction_merlin=c.fonction_merlin and a.fonction_id=c.fonction_id and a.ees_id=c.ees_id and a.ees_libl=c.ees_libl
            inner join
            (
            select fonction_merlin, fonction_id, ees_id, ees_libl,'MTS_'||fonction_merlin as new_fonction_merlin,'MTS' as dept from v_map_reseau_fonction_ees where lower(mts)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'RER_'||fonction_merlin as new_fonction_merlin,'RER' as dept from v_map_reseau_fonction_ees where lower(rer)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'SEM_'||fonction_merlin as new_fonction_merlin,'SEM' as dept from v_map_reseau_fonction_ees where lower(sem)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'SUR_'||fonction_merlin as new_fonction_merlin,'SUR' as dept from v_map_reseau_fonction_ees where lower(sur)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'Batimentaire' as new_fonction_merlin,'MRF' as dept from v_map_reseau_fonction_ees where lower(mrf)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'TRAM_'||fonction_merlin as new_fonction_merlin,'TRAM' as dept from v_map_reseau_fonction_ees where lower(tram)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'BUS_'||fonction_merlin as new_fonction_merlin,'BUS' as dept from v_map_reseau_fonction_ees where lower(bus)='x' and fonction_id is not null
            union all
            select fonction_merlin, fonction_id, ees_id, ees_libl,'Batimentaire' as new_fonction_merlin,'RRE' as dept from v_map_reseau_fonction_ees where lower(rre)='x' and fonction_id is not null
        ) b on a.fonction_merlin=b.fonction_merlin and a.fonction_id=b.fonction_id and a.ees_id=b.ees_id and a.ees_libl=b.ees_libl and c.dept=b.dept   
)

select * from final