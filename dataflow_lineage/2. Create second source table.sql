CREATE OR REPLACE TABLE hca-sandbox.lineage_samples.facility_metrics (
    facility_id INT64,
    quality_score INT64,
    last_inspection_date DATE
);

INSERT INTO hca-sandbox.lineage_samples.facility_metrics (facility_id, quality_score, last_inspection_date)
VALUES
    (1, 85, '2023-10-01'),
    (2, 92, '2023-09-15'),
    (3, 78, '2023-08-20');
