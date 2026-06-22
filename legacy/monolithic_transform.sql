-- ============================================================
-- LEGACY MONOLITHIC TRANSFORM (the "before")
-- Single-pass T-SQL: raw tables -> one denormalized reporting table.

-- Deliberately exhibits legacy anti-patterns:
--   - No layering (raw straight to final, no staging/Bronze-Silver-Gold)
--   - Inline cleaning/casting/logic tangled in one statement
--   - Mixed grain (per-registration rows + per-student aggregates duplicated)
--   - No reconciliation, no lineage, no idempotency discipline

-- This is what the medallion migration replaces.
-- ============================================================

-- (drop/recreate the output table - no idempotency discipline)
DROP TABLE IF EXISTS legacy_student_summary;

SELECT
    si.id_student,
    si.gender,
    si.region,
    si.highest_education,
    si.age_band,
    CASE WHEN si.imd_band IS NULL OR si.imd_band = '' THEN 'Unknown' ELSE si.imd_band END AS imd_band,
    CAST(sr.date_registration AS INT) AS date_registration,
    CASE WHEN sr.date_unregistration IS NOT NULL AND sr.date_unregistration <> '' 
     THEN 1 ELSE 0 END AS is_withdrawn,
    (SELECT ROUND(AVG(CAST(sa.score AS FLOAT)),2) AS avg_score FROM studentAssessment AS sa WHERE sa.id_student = si.id_student) AS avg_score,
    (SELECT SUM(CAST(sv.sum_click AS INT)) FROM studentVle AS sv WHERE sv.id_student = si.id_student) AS total_clicks
    -- ... cleaning + casting + derived fields go here (inline) ...
    -- ... aggregated score, aggregated clicks, withdrawal flag ...
INTO legacy_student_summary
FROM studentInfo si
LEFT JOIN studentRegistration AS sr
    ON sr.id_student = si.id_student 
    AND sr.code_module = si.code_module
    AND sr.code_presentation = si.code_presentation;


SELECT TOP 10 *
FROM legacy_student_summary;