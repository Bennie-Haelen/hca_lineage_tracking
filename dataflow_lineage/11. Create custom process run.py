import requests
from google.auth.transport.requests import Request
from google.auth import default

# Set variables
PROJECT_ID = "hca-sandbox"
LOCATION_MULTI = "us"
CUSTOM_LINEAGE_PROCESS_ID = "f70e6f72-b1b0-423a-84c3-8fb3347cbac3"
API_URL = f"https://us-datalineage.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION_MULTI}/processes/{CUSTOM_LINEAGE_PROCESS_ID}/runs"

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
    "displayName": "One time load",
    "startTime": "2024-11-20T14:14:11.238Z",
    "endTime": "2024-11-20T14:16:11.238Z",
    "state": "COMPLETED",
}

# Make POST request
response = requests.post(API_URL, headers=headers, json=payload)

# Print response
if response.status_code == 200:
    print("Run created successfully!")
    print(response.json())
else:
    print("Error:", response.status_code, response.text)
