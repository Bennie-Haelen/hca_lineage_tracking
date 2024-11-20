import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, ReadFromBigQuery

# Define the pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='hca-sandbox',
    region='us-central1',
    temp_location='gs://bennie_bucket_for_dataflow/temp',
    staging_location='gs://bennie_bucket_for_dataflow/staging'
)

# Define the BigQuery tables
facilities_table = 'hca-sandbox.lineage_samples.facilities'
metrics_table = 'hca-sandbox.lineage_samples.facility_metrics'
target_table = 'hca-sandbox.lineage_samples.facility_inspection_report'

# Define the Dataflow pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    # Read data from the facilities table
    facilities = (
        pipeline
        | 'Read Facilities' >> ReadFromBigQuery(table=facilities_table)
    )
    
    # Read data from the facility metrics table
    metrics = (
        pipeline
        | 'Read Facility Metrics' >> ReadFromBigQuery(table=metrics_table)
    )
    
    # Join the two tables on facility_id
    joined_data = (
        {'facilities': facilities, 'metrics': metrics}
        | 'Join Tables' >> beam.CoGroupByKey()
        | 'Transform Joined Data' >> beam.FlatMap(lambda row: [
            {
                'facility_id': row[0],
                'facility_name': row[1]['facilities'][0]['facility_name'],
                'city': row[1]['facilities'][0]['city'],
                'state': row[1]['facilities'][0]['state'],
                'quality_score': row[1]['metrics'][0]['quality_score'],
                'last_inspection_date': row[1]['metrics'][0]['last_inspection_date']
            }
        ])
    )
    
    # Write the result to the target BigQuery table
    joined_data | 'Write to BigQuery' >> WriteToBigQuery(
        target_table,
        schema='facility_id:INTEGER, facility_name:STRING, city:STRING, state:STRING, quality_score:INTEGER, last_inspection_date:DATE',
        write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE
    )
