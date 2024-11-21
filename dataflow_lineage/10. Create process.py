import requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from google.auth import default

# Set variables
PROJECT_ID = "hca-sandbox"
LOCATION_MULTI = "us"
API_URL = f"https://us-datalineage.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION_MULTI}/processes"

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
    "displayName": "Load facilities and metrics"
}

# Make POST request
response = requests.post(API_URL, headers=headers, json=payload)

# Print response
if response.status_code == 200:
    print("Process created successfully!")
    print(response.json())
else:
    print("Error:", response.status_code, response.text)
