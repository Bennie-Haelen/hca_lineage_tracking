import os
import json
import argparse
from pathlib import Path
from modules.Lineage import Lineage


def parse_arguments():
    parser = argparse.ArgumentParser(description="Lineage Management CLI")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--location_id", required=True, help="GCP Location ID")
    parser.add_argument("--process_id", required=True, help="Lineage Process ID")
    parser.add_argument("--run_id", required=True, help="Lineage Run ID")
    parser.add_argument("--lineage_event_id", required=True, help="Lineage Event ID")
    parser.add_argument("--output_file_name", required=True, help="Output file name for results")
    parser.add_argument("--action", required=True, choices=[
        "create_process", "create_run", "create_lineage", "show_lineage", "delete_lineage"],
        help="Action to perform: create_process, create_run, create_lineage, show_lineage, or delete_lineage")
    return parser.parse_args()

def print_pretty_json(data):
    print(json.dumps(data, indent=4))

# Example Usage
if __name__ == "__main__":

    # Parse arguments
    arguments = parse_arguments()
    project_id = arguments.project_id
    location_id = arguments.location_id
    base_url = "https://us-datalineage.googleapis.com/v1/"

    # Initialize Lineage object
    lineage = Lineage(project_id=project_id, location=location_id, base_url=base_url)

    # Perform action based on arguments
    match arguments.action:

        case "show_lineage":
            process_id = arguments.process_id
            run_id = arguments.run_id

            output_file = f"{ os.getcwd()}/Lineage Files/{project_id}-{location_id}-{process_id}-{run_id}.json"
            print(f"Output File: {output_file}")
            status_code, lineage_events = \
                lineage.ShowLineageEvents(process_id=process_id, run_id=run_id, output_file_path=output_file)
            
            if status_code == 200:
                print("Lineage Events:", print_pretty_json(lineage_events))
            else:
                print("Error:", status_code)

        case _:
            raise ValueError(f"Invalid action: {arguments.action}")
            
    # process_id = "08befbf89bf35f68c4d29cc721fa5a64"
    # run_id = "7b35d6080fda31ff494aa6b7a607e807"
       
    


    # # Create a process
    # process_response = lineage.CreateLineageProcess(process_id="process1", display_name="Sample Process")
    # print("Create Process Response:", process_response)

    # # Create a run
    # run_response = lineage.CreateLineageRun(process_id
    # ="process1", run_id="run1", start_time="2024-06-18T00:00:00Z")
    # print("Create Run Response:", run_response)

    # # Create an event
    # event_response = lineage.CreateLineageEvent(process_id="process1", run_id="run1", event_id="event1", event_type="START", asset_name="sample_asset")
    # print("Create Event Response:", event_response)

    # Show lineage events
    # output_file = f"{ os.getcwd()}/Lineage Files/{project_id}-{location_id}-{process_id}-{run_id}.json"
    # print(f"Output File: {output_file}")
    # status_code, lineage_events = \
    #     lineage.ShowLineageEvents(process_id=process_id, run_id=run_id, output_file_path=output_file)
    
    # if status_code == 200:
    #     print("Lineage Events:", print_pretty_json(lineage_events))
    # else:
    #     print("Error:", status_code)

    # # Delete an event
    # delete_status, delete_text = lineage.DeleteLineageEvent(process_id="process1", run_id="run1", event_id="event1")
    # print(f"Delete Event Status: {delete_status}, Response: {delete_text}")
