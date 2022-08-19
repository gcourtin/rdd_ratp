with v_organisation_reseau as (
    select * from {{ ref('net_organisation_reseau') }}
),

v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_reseau_fonction as (
    select * from {{ ref('net_reseau_fonction') }}
),

v_map_org_dept as (
    select * from {{ ref('net_map_org_dept') }}
),

final as (
    Select 
        m.organisation_id,m.departement,
        coalesce (o.fonction_id,f.fonction_id) fonction_id, 
        concat(nvl(m.departement,''),  coalesce (to_varchar(o.fonction_id),to_varchar(f.fonction_id))) as departement_fonction_id
        from v_map_org_dept m 
        inner join v_organisation o on o.organisation_id =m.organisation_id
        left join v_organisation_reseau r on m.organisation_id =r.or_organisation_id 
        left join v_reseau_fonction f on r.or_reseau_id = f.reseau_id
        where (o.fonction_id is not null or f.fonction_id is not null)
)

select * from final