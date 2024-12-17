import requests
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.auth import default

# Set variables
PROJECT_ID = "hca-sandbox"
LOCATION_MULTI = "us"
CUSTOM_LINEAGE_PROCESS_ID = "d39f6aecdaff344503bf35b5940aaf9e"
CUSTOM_LINEAGE_PROCESS_RUN_ID = "852b0862de04235c5889e3cf5db70665"
LINEAGE_EVENT = "6275b1e047328e0a0aff4e1f347b1746"
API_URL = f"https://us-datalineage.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION_MULTI}/processes/{CUSTOM_LINEAGE_PROCESS_ID}/runs/{CUSTOM_LINEAGE_PROCESS_RUN_ID}/lineageEvents/{LINEAGE_EVENT}"
OUTPUT_FILE = Path.home() / "OneDrive - Insight\projects\HACH\SourceFiles\hca_lineage_tracking\dataflow_lineage\custom_lineage_events.json"

# Ensure output directory exists
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# Get access token using default credentials
credentials, project = default()
credentials.refresh(Request())  
access_token = credentials.token

# Define headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

# Make GET request
response = requests.delete(API_URL, headers=headers)

# Save response to a JSON file
if response.status_code == 200:
    with open(OUTPUT_FILE, "w") as f:
        json.dump(response.json(), f, indent=2)
    print(f"Lineage events saved to: {OUTPUT_FILE}")

    # Print the saved content
    with open(OUTPUT_FILE, "r") as f:
        print(f.read())
else:
    print("Error:", response.status_code, response.text)
