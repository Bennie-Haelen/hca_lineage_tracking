SELECT
    creation_time,
    job_id,
    query,
    referenced_tables,
    destination_table
FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
    destination_table.dataset_id = 'lineage_samples'
    AND destination_table.table_id = 'target_hospitals'
    AND state = 'DONE'
ORDER BY
    creation_time DESC
LIMIT 1;

