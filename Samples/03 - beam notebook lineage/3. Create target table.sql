--
-- Create the target table
--
CREATE OR REPLACE TABLE `hca-sandbox.lineage_samples.facility_inspection_report_virtnb` (
    facility_id INT64,
    facility_name STRING,
    city STRING,
    state STRING,
    quality_score INT64,
    last_inspection_date DATE
);
