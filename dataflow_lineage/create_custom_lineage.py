import requests
from google.auth.transport.requests import Request
from google.auth import default

def create_custom_lineage(project_id, dataset_id, target_table, source_tables, start_time):
    # Set API URL
    api_url = f"https://{project_id}-datalineage.googleapis.com/v1/projects/{project_id}/locations/{dataset_id}/lineageEvents"

    # Get access token using default credentials
    credentials, project = default()
    credentials.refresh(Request())
    access_token = credentials.token

    # Define headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Build links payload
    links = [
        {
            "source": {
                "fullyQualifiedName": f"bigquery:{project_id}.{dataset_id}.{source_table}"
            },
            "target": {
                "fullyQualifiedName": f"bigquery:{project_id}.{dataset_id}.{target_table}"
            },
        }
        for source_table in source_tables
    ]

    # Define payload
    payload = {
        "links": links,
        "startTime": start_time,
    }

    # Make POST request
    response = requests.post(api_url, headers=headers, json=payload)

    # Print response
    if response.status_code == 200:
        print("Lineage event created successfully!")
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None

# Example usage
if __name__ == "__main__":
    project_id = "hca-sandbox"
    dataset_id = "lineage_samples"
    target_table = "facility_inspection_report"
    source_tables = ["facility_metrics", "another_source_table"]
    start_time = "2022-01-01T14:14:11.238Z"
    create_custom_lineage(project_id, dataset_id, target_table, source_tables, start_time)