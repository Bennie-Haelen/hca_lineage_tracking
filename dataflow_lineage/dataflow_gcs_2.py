from google.cloud import datacatalog_v1
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import uuid
from apache_beam.io import WriteToText

# def add_lineage_metadata():
#     """Add lineage metadata to Data Catalog outside of the pipeline."""
#     print("Adding lineage metadata...")
#     datacatalog_client = datacatalog_v1.DataCatalogClient()  # Initialize client here
#     # Perform Data Catalog operations here
#     pass

# # Add lineage metadata outside the pipeline context
# add_lineage_metadata()

# Define the pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='hca-sandbox',
    region='us-central1',
    temp_location='gs://bennie_bucket_for_dataflow/temp',
    staging_location='gs://bennie_bucket_for_dataflow/staging',
    # service_account_email='sa-hca-hin-automation@hca-sandbox.iam.gserviceaccount.com',
    # save_main_session=True
)

# Define the Dataflow pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    facilities = (
        pipeline
        | 'Read Facilities' >> ReadFromBigQuery(query="SELECT * FROM lineage_samples.facilities", use_standard_sql=True)
        | 'MapFacilities' >> beam.Map(lambda row: (row['facility_id'], row))
        )
    

    metrics = (
        pipeline
        | 'Read Metrics' >> ReadFromBigQuery(query="SELECT * FROM lineage_samples.facility_metrics", use_standard_sql=True)
        | 'MapMetrics' >> beam.Map(lambda row: (row['facility_id'], row))        
        )

    joined_data = (
        {'facilities': facilities, 'metrics': metrics}
        | 'Join Data' >> beam.CoGroupByKey()
        | 'Transform Data' >> beam.FlatMap(lambda row: [
            {
                # 'facility_id': row[0],
                # 'facility_name': row[1]['facilities'][0]['facility_name'],
                # 'city': row[1]['facilities'][0]['city'],
                # 'state': row[1]['facilities'][0]['state'],
                # 'quality_score': row[1]['metrics'][0]['quality_score'],
                # 'last_inspection_date': row[1]['metrics'][0]['last_inspection_date']
                'facility_id': row[0],
                'facility_name': row[1]['facilities'][0]['facility_name'] if row[1]['facilities'] else None,
                'city': row[1]['facilities'][0]['city'] if row[1]['facilities'] else None,
                'state': row[1]['facilities'][0]['state'] if row[1]['facilities'] else None,
                'quality_score': row[1]['metrics'][0]['quality_score'] if row[1]['metrics'] else None,
                'last_inspection_date': row[1]['metrics'][0]['last_inspection_date'] if row[1]['metrics'] else None
            }
        ])
    )

    # (joined_data | 'Write to GCS' >>  WriteToText('gs://bennie_bucket_for_dataflow/temp/joined_data/output_1', file_name_suffix='.csv', shard_name_template=''))
    joined_data | 'Write to BigQuery' >> WriteToBigQuery(
        table='hca-sandbox:lineage_samples.facility_inspection_report',
        schema='facility_id:INTEGER, facility_name:STRING, city:STRING, state:STRING, quality_score:INTEGER, last_inspection_date:DATE',
        write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE
    )
