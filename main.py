import os
import json
import argparse
from pathlib import Path
from modules.Lineage import Lineage


def parse_arguments():
    parser = argparse.ArgumentParser(description="Lineage Management CLI")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--location_id", required=True, help="GCP Location ID")
    parser.add_argument("--dataset_id", required=False, help="Dataset ID")
    parser.add_argument("--table_id", required=False, help="Table ID")
    parser.add_argument("--process_id", required=False, help="Lineage Process ID")
    parser.add_argument("--run_id", required=False, help="Lineage Run ID")
    parser.add_argument("--lineage_event_id", required=False, help="Lineage Event ID")
    parser.add_argument("--output_file_name", required=False, help="Output file name for results")
    parser.add_argument("--action", required=True, choices=[
        "create_process", 
        "create_run", 
        "create_lineage", 
        "show_lineage", 
        "delete_lineage", 
        "delete_all_target_lineage",
        "show_target_lineage", 
        'show_source_lineage',
        'retrieve_processes_for_link',
        "retrieve_runs_for_process"],
        help="Action to perform: create_process, create_run, create_lineage, show_lineage, show_source_lineage, show_target_lineage, delete_lineage, retrieve_processes_for_link, or delete_all_target_lineage")
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
            status_code, lineage_events = lineage.ShowLineageEvents(process_id=process_id, run_id=run_id, output_file_path=output_file)
            
            if status_code == 200:
                print(f"status: code: {status_code}")
            else:
                print("Error:", status_code)


        case "show_target_lineage":
            dataset_id = arguments.dataset_id
            table_id = arguments.table_id

            status_code, links = lineage.SearchTargetLineage(dataset_id, table_id)
            print_pretty_json(links)

        case "show_source_lineage":
            dataset_id = arguments.dataset_id
            table_id = arguments.table_id

            status_code, links = lineage.SearchSourceLineage(dataset_id, table_id)
            print_pretty_json(links)

        case "delete_lineage":
            process_id = arguments.process_id
            run_id = arguments.run_id
            lineage_event_id = arguments.lineage_event_id

            status_code, response = lineage.DeleteLineageEvent(process_id=process_id, run_id=run_id, lineage_event_id=lineage_event_id)
            print(f"Delete Event Status: {status_code}, Response: {response}")

        case "delete_all_target_lineage":
            dataset_id = arguments.dataset_id
            table_id = arguments.table_id

            status_code, response = lineage.delete_all_target_lineage(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
            # print(f"Delete All Target Lineage Status: {status_code}, Response: {response}")

        case "retrieve_processes_for_link":
            link_name = "projects/624374607135/locations/us/links/p:9e3bba03f847d8fe00729c7f6ff841c2"
            process = lineage.RetrieveProcessForLink(link_name)
            print(f"Process: {process}")

        case "retrieve_runs_for_process":
            process_id = "f70e6f72-b1b0-423a-84c3-8fb3347cbac3"
            runs = lineage.RetrieveAllRunsForProcess(process_id)
            print_pretty_json(runs)

        case _:
            raise ValueError(f"Invalid action: {arguments.action}")
            
