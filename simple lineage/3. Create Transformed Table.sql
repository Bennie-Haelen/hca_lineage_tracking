-- Transform the data and create the target table
CREATE OR REPLACE TABLE `hca-sandbox.lineage_samples.target_hospitals` AS
SELECT
    facility_id,
    UPPER(facility_name) AS normalized_facility_name,
    city,
    state,
    zip_code,
    CASE
        WHEN state IN ('WA', 'OR', 'CA') THEN 'West'
        WHEN state IN ('IL', 'NY') THEN 'North'
        WHEN state IN ('TX') THEN 'South'
        ELSE 'Other'
    END AS region
FROM
    hca-sandbox.lineage_samples.source_facilities
WHERE
    LOWER(facility_type) LIKE '%hospital%';