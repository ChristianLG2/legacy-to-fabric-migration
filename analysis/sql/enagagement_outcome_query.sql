-- Pulls student+module grain engagement and outcome data for the engagement/withdrawal correlation analysis.
-- fact_vle is aggregated to student+module first (it's stored at a finer student + module + day grain) to match fact_registration's grain before joining 
-- joining at the wrong grain would fan out the result.

WITH student_clicks AS (
    SELECT student_sk, module_sk, SUM(total_clicks) AS total_clicks
    FROM fact_vle
    GROUP BY student_sk, module_sk
)
SELECT
    r.student_sk,
    r.module_sk,
    r.is_withdrawn,
    ISNULL(c.total_clicks, 0) AS total_clicks
FROM fact_registration r
LEFT JOIN student_clicks c
    ON r.student_sk = c.student_sk AND r.module_sk = c.module_sk
