import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io import WriteToText

# Define the pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='hca-sandbox',
    region='us-central1',
    temp_location='gs://bennie_bucket_for_dataflow/temp',
    staging_location='gs://bennie_bucket_for_dataflow/staging'
)

# Define the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Read data from BigQuery
    input_data = (p 
                  | 'ReadFromBigQuery' >> ReadFromBigQuery(query='SELECT *  FROM hca_e360_test.employee', use_standard_sql=True)
                 )
    
    # Write data to GCS as CSV
    (input_data 
     | 'WriteToGCS' >> WriteToText('gs://bennie_bucket_for_dataflow/temp/testoutput/output', file_name_suffix='.csv', shard_name_template='')
    )

# Run the pipeline
if __name__ == '__main__':
    print("Running the Dataflow job...")