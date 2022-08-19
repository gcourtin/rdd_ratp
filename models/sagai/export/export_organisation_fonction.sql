with v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_int_org_fonction1 as (
    select * from {{ ref('int_org_fonction1') }}
),

v_int_org_fonction2 as (
    select * from {{ ref('int_org_fonction2') }}
),

final as (
     Select
         om.organisation_id Identifiant,
         listagg(distinct int2.fonction_merlin,',') fonction_merlin
     from  v_organisation om
     left join (select int2.organisation_id,int2.departement,int2.fonction_id,int1.departement_id_fonction,int1.fonction_merlin  from  v_int_org_fonction2 int2
     left join v_int_org_fonction1 int1 on int1.departement_id_fonction = int2.departement_fonction_id) int2 on int2.organisation_id = om.organisation_id 
     group by om.organisation_id
)

select * from final