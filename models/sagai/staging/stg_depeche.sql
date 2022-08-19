
with source as (

    select * from {{ source('sagai_raw', 'depeche') }}

),

renamed as (

    select
        depeche_date,
        depeche_num,
        depeche_statut,
        depeche_ind,
        depeche_statut_nfe,
        depeche_date_cons,
        depeche_date_an,
        depeche_date_rspt,
        depeche_date_nftf,
        depeche_date_nfe,
        depeche_commentaire_exploitant

    from source

)

select * from renamed