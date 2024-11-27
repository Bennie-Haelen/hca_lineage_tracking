from google.cloud import datacatalog_v1
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import uuid
from apache_beam.io import WriteToText

 

# Define the pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='hca-sandbox',
    region='us-central1',
    temp_location='gs://bennie_bucket_for_dataflow/temp',
    staging_location='gs://bennie_bucket_for_dataflow/staging',
    service_account_email='lineage-sand@hca-sandbox.iam.gserviceaccount.com',
    # save_main_session=True
)

# Define the Dataflow pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    joined_data = (
        pipeline
        | 'Read Facilities and metrics' >> ReadFromBigQuery(query="SELECT a.facility_id,a.facility_name,a.city,a.state,b.quality_score,b.last_inspection_date FROM lineage_samples.facilities a LEFT JOIN lineage_samples.facility_metrics b ON a.facility_id = b.facility_id", use_standard_sql=True)         
        )
     

    # (joined_data | 'Write to GCS' >>  WriteToText('gs://bennie_bucket_for_dataflow/temp/joined_data/output_1', file_name_suffix='.csv', shard_name_template=''))
    joined_data | 'Write to BigQuery' >> WriteToBigQuery(
        table='hca-sandbox:lineage_samples.facility_inspection_report',
        schema='facility_id:INTEGER, facility_name:STRING, city:STRING, state:STRING, quality_score:INTEGER, last_inspection_date:DATE',
        write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE
    )
