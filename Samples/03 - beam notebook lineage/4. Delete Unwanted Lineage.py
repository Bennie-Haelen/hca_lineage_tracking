import os
import sys

# Ensure the modules path is added to the system path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import the Lineage class from the modules directory
from modules.Lineage import Lineage

# Constants for configuration
LOCATION_ID = "us"  # GCP region
PROJECT_ID = "hca-sandbox"  # GCP project ID
BASE_URL = "https://us-datalineage.googleapis.com/v1/"  # Lineage API base URL
GCS_BUCKET_PREFIX = "temp/bq_load"  # Prefix for GCS bucket paths

# Initialize the Lineage class with project and location configurations
lineage = Lineage(project_id=PROJECT_ID, location=LOCATION_ID, base_url=BASE_URL)

# Define the target dictionary list specifying tables and lineage sources
target_dictionary_list = [
    {
        "table_id": "lineage_samples.facilities_virtnb",
        "source_tables": [
            "bigquery:hca-sandbox.beam_temp_dataset_b12eb8b9bef0431b8eeaf3ed6ef262a1"
        ]
    },
    {
        "table_id": "lineage_samples.facility_inspection_report_virtnb",
        "gcs_buckets": [
            "temp/bq_load/77020113c54a482dbef014932aa33d88"
        ]
    },
    {
        "table_id": "lineage_samples.facility_metrics_virtnb",
        "source_tables": [
            "bigquery:hca-sandbox.beam_temp_dataset_d682f04e544c47c2aff790bcafacc30c"
        ]
    }
]

try:
    # Attempt to delete lineage events for the specified targets
    lineage.delete_lineage_from_target_dictionary(target_dictionary_list)
    print("All lineage events have been successfully deleted.")

except Exception as e:
    # Handle any errors that occur during lineage deletion
    print(f"Error during lineage deletion: {e}")
