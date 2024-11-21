import requests
from google.auth.transport.requests import Request
from google.auth import default

# Set variables
PROJECT_ID = "hca-sandbox"
LOCATION_MULTI = "us"
CUSTOM_LINEAGE_PROCESS_ID = "f70e6f72-b1b0-423a-84c3-8fb3347cbac3"
CUSTOM_LINEAGE_PROCESS_RUN_ID = "96afd59c-e92c-470d-ba3c-29fcba98942e"
API_URL = f"https://us-datalineage.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION_MULTI}/processes/{CUSTOM_LINEAGE_PROCESS_ID}/runs/{CUSTOM_LINEAGE_PROCESS_RUN_ID}/lineageEvents"

# Get access token using default credentials
credentials, project = default()
credentials.refresh(Request())
access_token = credentials.token

# Define headers and payload
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}
payload = {
    "links": [
        {
            "source": {
                "fullyQualifiedName": f"bigquery:hca-sandbox.lineage_samples.facility_metrics"
            },
            "target": {
                "fullyQualifiedName": "bigquery:hca-sandbox.lineage_samples.facility_inspection_report"
            },
        }
    ],
    "startTime": "2022-01-01T14:14:11.238Z",
}

# Make POST request
response = requests.post(API_URL, headers=headers, json=payload)

# Print response
if response.status_code == 200:
    print("Lineage event created successfully!")
    print(response.json())
else:
    print("Error:", response.status_code, response.text)
