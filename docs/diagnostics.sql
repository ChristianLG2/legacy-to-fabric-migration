/* ============================================================
   Legacy DW — Data Diagnostics
   Purpose: inspect raw OULAD tables in legacy_dw before Silver
   Findings feed Silver cleaning + Gold modeling decisions
   ============================================================ */

-- Check: How many rows landed in each of the 7 source tables?
--        (load completeness + identify the high-volume table)
SELECT 'assessments' AS table_name, COUNT(*) AS row_count FROM assessments
UNION ALL SELECT 'courses', COUNT(*) FROM courses
UNION ALL SELECT 'studentAssessment', COUNT(*) FROM studentAssessment
UNION ALL SELECT 'studentInfo', COUNT(*) FROM studentInfo
UNION ALL SELECT 'studentRegistration', COUNT(*) FROM studentRegistration
UNION ALL SELECT 'studentVle', COUNT(*) FROM studentVle
UNION ALL SELECT 'vle', COUNT(*) FROM vle;
-- Finding: studentVle = 10,655,280 rows — dwarfs all others (next largest
--          studentAssessment = 173,912; rest in the hundreds/thousands).
-- Decision: studentVle is THE table to optimize (partitioning, OPTIMIZE/V-Order).
--           No tuning effort wasted on small tables. Counts are also the
--           reconciliation baseline for validating each medallion layer.


-- Check: Are students unique in these tables, or do they repeat?
--        (total rows vs distinct id_student)
SELECT 'studentRegistration' AS tablename, COUNT(DISTINCT id_student) AS unique_students
FROM studentRegistration
UNION ALL SELECT 'studentInfo', COUNT(DISTINCT id_student) FROM studentInfo;
-- Finding: 32,593 rows but only 28,785 distinct students in each table.
--          Students appear multiple times (they re-register across modules/presentations).
-- Decision: Grain is one row per student-per-registration, NOT per student.
--           id_student alone is not a key — need composite
--           (id_student + code_module + code_presentation).
--           Repeating students = SCD2 candidate for dim_student in Gold.


-- Check: Do studentInfo and studentRegistration contain the SAME set of
--        registrations, or does one have records the other lacks?
--        (anti-join, both directions, on the composite key)
SELECT COUNT(*) AS in_reg_not_in_info
FROM studentRegistration AS r
LEFT JOIN studentInfo AS i
    ON i.id_student = r.id_student
    AND i.code_module = r.code_module
    AND i.code_presentation = r.code_presentation
WHERE i.id_student IS NULL;

SELECT COUNT(*) AS in_info_not_in_reg
FROM studentInfo AS i
LEFT JOIN studentRegistration AS r
    ON r.id_student = i.id_student
    AND r.code_module = i.code_module
    AND r.code_presentation = i.code_presentation
WHERE r.id_student IS NULL;
-- Finding: 0 in both directions — every registration has a matching info
--          record and vice versa. Referential integrity is intact.
-- Decision: studentInfo <-> studentRegistration join is safe on the composite
--           key; no orphan handling or quarantine needed for THIS relationship
--           in Silver. (Knowing where data is clean tells me where NOT to spend effort.)
 

-- Check: Is imd_band missing or inconsistently formatted? (dimension column users will slice by)
SELECT imd_band, COUNT(*) as imd_count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM studentInfo
GROUP BY imd_band
ORDER BY imd_count DESC;
-- Finding: Nulls are true Null values, not empty strings.
--  10-20 does not contain % sign, formatting incosistency.
--  Nulls represent 3.4n percent of the data
-- Decision: (1) Impute 1,111 NULL imd_band -> "Unknown" 
-- (3.4% is low, safe to impute not drop). 
-- (2) Standardize band format ("10-20" -> "10-20%") so GROUP BY/reports don't split the category.


-- Check: What share of registrations are withdrawals?
--        (date_unregistration populated = withdrew; drives keep-vs-drop decision)
SELECT CASE WHEN date_unregistration IS NULL OR date_unregistration = '' 
            THEN 'registered' ELSE 'unregistered' END AS registration_status,
       COUNT(*) AS status_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS status_percentage
FROM studentRegistration
GROUP BY CASE WHEN date_unregistration IS NULL OR date_unregistration = '' 
              THEN 'registered' ELSE 'unregistered' END;
-- Finding: ~30% of registrations are withdrawals. [note empty-form: NULL only / mixed]
-- Decision: Keep withdrawn rows, add is_withdrawn flag. Dropping ~30% would gut the
--           dataset and erase the most analytically interesting cohort (retention analysis).