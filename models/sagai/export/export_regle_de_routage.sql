with net_regle as (
    select * from {{ ref('net_regle') }}
),

int_regle_mots_clefs as (
    select * from {{ ref('int_regle_mots_clefs') }}
),

int_regle_anomalie as (
    select * from {{ ref('int_regle_anomalie') }}
),

int_regle_lieu as (
    select * from {{ ref('int_regle_lieu') }}
),

int_regle_ees as (
    select * from {{ ref('int_regle_ees') }}
),

int_regle_action as (
    select * from {{ ref('int_regle_action') }}
),

int_regle_copie as (
    select * from {{ ref('int_regle_copie') }}
),

final as (
    select
        r.regle_id as Identifiant,
        r.regle_libl as Libelle,
        r.regle_desc as Description,
        true as Actif,
        r.regle_dat_deb as Date_debut,
        r.regle_dat_fin as Date_fin,
        null as Plage_horaire,
        null as Heure_de_debut,
        null as Heure_de_fin,
        r.regle_poids as Poids,
        rl.elo_id as Lieux,
        re.ees_id as EES,
        m.mot_clef as Equipement,
        ra.anomalie_id as Anomalie,
        r.organisation_id as Intervenant_principal,
        rac.organisation_id as Intervenant_pour_action,
        rc.organisation_id as Intervenant_en_copie
    from net_regle r
    left join int_regle_anomalie ra on ra.regle_id=r.regle_id
    left join int_regle_ees re on re.regle_id=r.regle_id
    left join int_regle_lieu rl on rl.regle_id=r.regle_id
    left join int_regle_action rac on rac.regle_id=r.regle_id
    left join int_regle_mots_clefs m on m.regle_id=r.regle_id
    left join int_regle_copie rc on rc.regle_id=r.regle_id 
    where Lieux is not null  
)

select * from final