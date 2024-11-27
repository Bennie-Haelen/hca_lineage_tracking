import requests
from google.auth import default
from google.auth.transport.requests import Request

def create_process(project_id, location_multi, display_name):
    # Set variables
    api_url = f"https://{location_multi}-datalineage.googleapis.com/v1/projects/{project_id}/locations/{location_multi}/processes"

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
        "displayName": display_name 
    }

    # Make POST request
    response = requests.post(api_url, headers=headers, json=payload)

    # Print response
    if response.status_code == 200:
        print("Process created successfully!")
        return response.json()['name'].split("/")[-1], response.json()['name']
    else:
        print("Error:", response.status_code, response.text)
        return None

# Example usage
if __name__ == "__main__":
    project_id = "hca-sandbox"
    location_multi = "us"
    display_name = "Load facilities and metrics"
    create_process(project_id, location_multi, display_name)