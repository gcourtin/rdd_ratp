with v_organisation_zone as (
    select * from {{ ref('net_organisation_zone') }}
),

v_organisation as (
    select * from {{ ref('net_organisation') }}
),

v_export_groupe_de_lieux as (
    select * from {{ ref('export_groupe_de_lieux') }}
),

final as (
     Select
        identifiant as Identifiant,
        listagg(distinct parent,',') as Nom 
     from (
        select a.identifiant,b.parent from (
            select a.organisation_id as Identifiant, coalesce(c.oz_zonegeo_id,a.zonegeo_id) zonegeo_id from v_organisation a
            left join v_organisation_zone c on a.organisation_id = c.oz_organisation_id
            where coalesce(c.oz_zonegeo_id,a.zonegeo_id) is not null
        ) a 
         inner join (
             select distinct replace(z.value,'_Z') as zonegeo_id, z.value as parent 
             from v_export_groupe_de_lieux v, table(split_to_table(v.parent,',')) z
         ) b on a.zonegeo_id=b.zonegeo_id
     )  
     group by identifiant
)

select * from final