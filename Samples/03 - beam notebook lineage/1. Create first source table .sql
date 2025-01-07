--
-- Create the first source table
--
CREATE OR REPLACE TABLE `hca-sandbox.lineage_samples.facilities_virtnb` (
    facility_id INT64,
    facility_name STRING,
    city STRING,
    state STRING
);

--
-- Insert data into the first source table
--
INSERT INTO `hca-sandbox.lineage_samples.facilities_virtnb` (facility_id, facility_name, city, state)
VALUES
    (1, 'General Hospital', 'Chicago', 'IL'),
    (2, 'Downtown Clinic', 'New York', 'NY'),
    (3, 'Community Care Center', 'San Francisco', 'CA');

