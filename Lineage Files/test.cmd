gcloud dataflow sql query 
    'SELECT 
        f.facility_id, 
        f.facility_name, 
        f.city, 
        f.state, 
        m.quality_score,  
        m.last_inspection_date 
    FROM  
        bigquery.table.hca-sandbox.lineage_samples.facilities_sql AS f  
    LEFT JOIN   
        bigquery.table.hca-sandbox.lineage_samples.facility_metrics_sql AS m  
    ON  
        f.facility_id = m.facility_id' 
    --job-name=bennie-dataflow-sql-test 
    --region=us-central1  
    --bigquery-dataset=hca-sandbox:lineage_samples 
    --bigquery-table=facility_inspection_report_sql 
