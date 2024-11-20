-- Create the source table
CREATE OR REPLACE TABLE hca-sandbox.lineage_samples.source_facilities (
    facility_id INT64,
    facility_name STRING,
    facility_type STRING,
    city STRING,
    state STRING,
    zip_code STRING
);
