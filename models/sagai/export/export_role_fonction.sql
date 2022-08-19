with v_net_fonction as (
    select * from {{ ref('net_fonction') }}
),

v_net_reseau_fonction as (
    select * from {{ ref('net_reseau_fonction') }}
),

v_net_reseau as (
    select * from {{ ref('net_reseau') }}
),

v_net_map_fonction_merlin as (
    select * from {{ ref('net_map_fonction_merlin') }}
),

final as (
    select 
        nom Nom,
        case 
        when nom = 'Transport' then 'Fonction EES spécifique pour MTS'
        when nom = 'Batimentaire' then 'Fonction EES spécifique pour MRF et RRE'
        when contains(nom,'_') then 'Fonction EES spécifique pour '||split_part(nom,'_',1)
        else 'Fonction EES en attente de classement'
        end Description
    from (
        select distinct trim(coalesce(m.fonction_libc_merlin,f.fonction_libc)) nom
        from v_net_fonction f 
        inner join v_net_reseau_fonction rf on f.fonction_id =rf.fonction_id 
        inner join v_net_reseau r on rf.reseau_id =r.reseau_id 
        left join v_net_map_fonction_merlin m on trim(f.fonction_libc) = trim(m.fonction_libc_sagai) 
        where r.reseau_libc = 'Merlin'
        ) 
    where Nom not like 'BUS%'
)

select * from final