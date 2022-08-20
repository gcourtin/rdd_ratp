with v_mots_clefs as (
    select * from {{ ref('mots_clefs_detail') }} 
),

final as (
    select 'Tous' ligne,
        count(mot_clef) mot_clef,
        sum(elo_id_null) elo_id_null,
        sum(ees_id_null) ees_id_null,
        sum(org_id_null) org_id_null,
        sum(code_gmao_not_null) code_gmao_not_null,
        sum(lieu_bus) lieu_bus,
        sum(ees_npu) ees_npu,
        sum(ees_abs) ees_abs,
        sum(org_abs) org_abs,
        sum(rpt_abs) rpt_abs,
        sum(mc_non_repris) mc_non_repris
    from v_mots_clefs
    union all
    select 'Hors : lieux BUS et lieu absent et ELO_ID null' ligne,
        count(mot_clef) mot_clef,
        sum(elo_id_null) elo_id_null,
        sum(ees_id_null) ees_id_null,
        sum(org_id_null) org_id_null,
        sum(code_gmao_not_null) code_gmao_not_null,
        sum(lieu_bus) lieu_bus,
        sum(ees_npu) ees_npu,
        sum(ees_abs) ees_abs,
        sum(org_abs) org_abs,
        sum(rpt_abs) rpt_abs,
        sum(mc_non_repris) mc_non_repris
    from v_mots_clefs
    where lieu_bus=0 and rpt_abs=0 AND elo_id_null=0
    union all
    select 'Hors NPU EES et EES Absent et EES_ID null' ligne,
        count(mot_clef) mot_clef,
        sum(elo_id_null) elo_id_null,
        sum(ees_id_null) ees_id_null,
        sum(org_id_null) org_id_null,
        sum(code_gmao_not_null) code_gmao_not_null,
        sum(lieu_bus) lieu_bus,
        sum(ees_npu) ees_npu,
        sum(ees_abs) ees_abs,
        sum(org_abs) org_abs,
        sum(rpt_abs) rpt_abs,
        sum(mc_non_repris) mc_non_repris
    from v_mots_clefs
    where ees_npu=0 and ees_abs=0 AND ees_id_null=0
    union all
    select 'Hors : BUS et lieux absent ou null et NPU EES et EES Absent ou null' ligne,
        count(mot_clef) mot_clef,
        sum(elo_id_null) elo_id_null,
        sum(ees_id_null) ees_id_null,
        sum(org_id_null) org_id_null,
        sum(code_gmao_not_null) code_gmao_not_null,
        sum(lieu_bus) lieu_bus,
        sum(ees_npu) ees_npu,
        sum(ees_abs) ees_abs,
        sum(org_abs) org_abs,
        sum(rpt_abs) rpt_abs,
        sum(mc_non_repris) mc_non_repris
    from v_mots_clefs
    where ees_npu=0 and ees_abs=0 and lieu_bus=0 and rpt_abs=0 AND elo_id_null=0 AND ees_id_null=0
)

select * from final