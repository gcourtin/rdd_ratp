{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="public",
      identifier="export_ees"
) -%}

{% set dbt_relation=ref('export_ees') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["SOUS_TYPE","LIBELLE","RESEAUX_FONCTIONS","ELO_ID","COMMENTAIRE","X_PEAGE","URL_UNC", "INVENTAIRE_AUTORISE", "ACTIF", "STATION"],
    primary_key="id_sagai"
) }}