{% test agg_relationships(model, column_name, field, to) %}

with parent as (

    select
        {{ field }} as id

    from {{ to }} 

),

child as (

    select
        case when length(z.value)=0 then null else z.value end as id

    from {{ model }} a,table(split_to_table ( {{ column_name }},',')) z
)

select *
from child
where id is not null
  and id not in (select id from parent)

{% endtest %}