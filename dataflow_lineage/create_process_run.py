import datetime
import requests
from google.auth.transport.requests import Request
from google.auth import default
from create_process import create_process
import datetime
import time
def create_process_run(project_id, location_multi, process_id, display_name, start_time, end_time, state):
    # Set API URL
    api_url = f"https://{location_multi}-datalineage.googleapis.com/v1/projects/{project_id}/locations/{location_multi}/processes/{process_id}/runs"

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
        "displayName": display_name,
        "startTime": start_time,
        # "endTime": end_time,
        # "state": state,
        "startTime": start_time.isoformat() + 'Z',
        "endTime": end_time.isoformat() + 'Z',
    }

    # Make POST request
    response = requests.post(api_url, headers=headers, json=payload)

    # Print response
    if response.status_code == 200:
        print("Run created successfully!")
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None

# Example usage
if __name__ == "__main__":
    
    project_id = "hca-sandbox"
    location_multi = "us"
    display_name = "One time load"    
    process_id, process_name = create_process( project_id, location_multi, display_name) 
    print(process_id)   
    # process_id = process_id_val
    # start_time = "2024-11-20T14:14:11.238Z" datetime.now().isoformat()
    # end_time = "2024-11-20T14:16:11.238Z" 
    start_time = datetime.datetime.now()
    end_time = datetime.datetime.now()
    state = "COMPLETED"
    print(create_process_run(project_id, location_multi, process_id, display_name, start_time, end_time, state))