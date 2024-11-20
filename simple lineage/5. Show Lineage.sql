SELECT
    creation_time,
    user_email,
    query,
    referenced_table_schema AS source_schema,
    referenced_table_name AS source_table,
    destination_table_schema AS target_schema,
    destination_table_name AS target_table
FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
    state = "DONE"
    AND destination_table_name = 'target_hospitals'
ORDER BY
    creation_time DESC;
