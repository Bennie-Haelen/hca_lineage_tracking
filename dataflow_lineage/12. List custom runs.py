import requests
import json
from google.auth.transport.requests import Request
from google.auth import default
from pathlib import Path

# Set variables
PROJECT_ID = "hca-sandbox"
LOCATION_MULTI = "us"
CUSTOM_LINEAGE_PROCESS_ID = "f70e6f72-b1b0-423a-84c3-8fb3347cbac3"
API_URL = f"https://us-datalineage.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION_MULTI}/processes/{CUSTOM_LINEAGE_PROCESS_ID}/runs"
OUTPUT_FILE = Path.home() / "OneDrive - Insight\projects\HACH\SourceFiles\hca_lineage_tracking\dataflow_lineage/custom_lineage_run.json"

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
response = requests.get(API_URL, headers=headers)

# Save response to a JSON file
if response.status_code == 200:
    with open(OUTPUT_FILE, "w") as f:
        json.dump(response.json(), f, indent=2)
    print(f"Lineage runs saved to: {OUTPUT_FILE}")
else:
    print("Error:", response.status_code, response.text)
