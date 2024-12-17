-- Insert data into the source table
INSERT INTO `hca-sandbox.lineage_samples.source_facilities` (
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    zip_code
)
VALUES 
    (1, 'General Hospital', 'Hospital', 'Chicago', 'IL', '60601'),
    (2, 'Downtown Clinic', 'Clinic', 'New York', 'NY', '10001'),
    (3, 'Community Care Center', 'Health Center', 'San Francisco', 'CA', '94101'),
    (4, 'Pediatric Clinic', 'Clinic', 'Austin', 'TX', '73301'),
    (5, 'City Hospital', 'Hospital', 'Seattle', 'WA', '98101');
