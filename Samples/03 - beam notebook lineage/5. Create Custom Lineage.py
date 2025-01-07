import os
import sys
import json
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..\..')))
from modules.Lineage import Lineage

LOCATION_ID = "us"
PROJECT_ID  = "hca-sandbox"
DATASET_ID  = "lineage_samples"
BASE_URL    = "https://us-datalineage.googleapis.com/v1/"

# Create an instance of the Lineage Class
lineage = Lineage(project_id=PROJECT_ID, location=LOCATION_ID, base_url=BASE_URL)  

#
# Step 1 - Create a Lineage Process and extract the process_id
#
response_code, process_info = lineage.create_lineage_process("1-Bennie Notebook Lineage for facility_inspection_report_virtnb")
print(json.dumps(process_info, indent=2))

# Extract the Process Id
process_full_name = process_info["name"]
process_id = process_full_name.split('/')[-1]
print(f"Process full name: {process_full_name}, process_id: {process_id}")


#
# Step 2 - Create a Lineage Run and extract the run_id
#
end_time = datetime.utcnow()
start_time = end_time - timedelta(minutes=2)
print(f"start_time: {start_time}, end_time: {end_time}, type start time: {type(start_time)}, type end time: {type(end_time)}")

run_display_name = "Bennie Lineage Test Run"
response_code, run_info = lineage.create_lineage_run(process_id, run_display_name, start_time, end_time)
print(json.dumps(run_info, indent=2))

run_full_name = run_info["name"]
run_id = run_full_name.rsplit('/', 1)[-1]
print(f"Run full name: {run_full_name}, run_id: {run_id}")

#
# Step 3 - Create the Lineage Event
#
response_code, lineage_info =                                     \
    lineage.create_lineage_event(                                 \
        process_id=process_id,                                    \
        run_id=run_id,                                            \
        dataset_id=DATASET_ID,                                    \
        sources=["facilities_virtnb", "facility_metrics_virtnb"], \
        target="facility_inspection_report_virtnb",               \
        start_time=start_time,                                    \
        end_time=end_time)

# Output the created lineage information
print(json.dumps(lineage_info, indent=2))

