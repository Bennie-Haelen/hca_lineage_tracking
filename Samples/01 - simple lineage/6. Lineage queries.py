import os
import sys
import json
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..\..')))
from modules.Lineage import Lineage

LOCATION_ID = "us"
PROJECT_ID  = "hca-sandbox"
DATASET_ID  = "lineage_samples"
TABLE_ID    = "target_hospitals"
BASE_URL    = "https://us-datalineage.googleapis.com/v1/"

# Create an instance of the Lineage Class
lineage = Lineage(project_id=PROJECT_ID, location=LOCATION_ID, base_url=BASE_URL)    

# Get the lineage process for our "01 - simple lineage" process
process_id = "08befbf89bf35f68c4d29cc721fa5a64"
status_code, process_details = lineage.get_process(process_id)

if status_code == 200:
    print("Process Details:")
    print(json.dumps(process_details, indent=2))

# Get the lineage runs for our "01 - simple lineage" process
run_id = "7b35d6080fda31ff494aa6b7a607e807"
status_code, run_details = lineage.get_process_run(process_id, run_id)

if status_code == 200:
    print("\nRun Details:")
    print(json.dumps(run_details, indent=2))

# Get the target lineage for our table
status_code, search_target_lineage = \
    lineage.search_target_lineage(dataset_id=DATASET_ID, table_id=TABLE_ID)

if status_code == 200:
    print("\nTarget Lineage:")
    print(json.dumps(search_target_lineage, indent=2))

# Get an array of the source table(s) for our target table
status_code, tables = lineage.get_source_tables_for_table(dataset_id=DATASET_ID, table_id=TABLE_ID)

if status_code == 200:
    print(f"\nSource Tables: {tables}")

# Get a full source description of the table, including the source tables, 
# columns, and the lineage process that created the table
result = lineage.get_full_source_description_for_table(dataset_id=DATASET_ID, table_id=TABLE_ID)
print(f"\nFull Source Description:\n{result}")