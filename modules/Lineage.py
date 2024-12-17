import requests
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.auth import default

class Lineage:
    def __init__(self, project_id, location, base_url, credentials=None):
        """
        Initialize the Lineage class.
        :param project_id: GCP Project ID
        :param location: GCP Location/Region
        :param base_url: Base URL for Lineage API
        """
        self.project_id = project_id
        self.location = location
        self.base_url = base_url.rstrip('/')
        self.credentails = credentials

    def _get_acccess_token(self):
        # Get access token using default credentials
        credentials, project = default()
        credentials.refresh(Request())  
        access_token = credentials.token

        return access_token

    def _get_headers(self):
        """
        Helper to create headers for HTTP requests.
        Replace this with an actual Authorization method.
        """

        access_token = self._get_acccess_token()
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"  # Replace with valid token logic
        }


    def CreateLineageProcess(self, process_id, display_name):
        """
        Create a new lineage process.
        :param process_id: ID for the lineage process
        :param display_name: Human-readable name for the process
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}"
        payload = {
            "displayName": display_name,
        }
        response = requests.post(url, json=payload, headers=self._get_headers())
        return response.json()

    def CreateLineageRun(self, process_id, run_id, start_time):
        """
        Create a new lineage run under a specific process.
        :param process_id: ID of the lineage process
        :param run_id: ID for the lineage run
        :param start_time: Start time of the run
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}"
        payload = {
            "startTime": start_time,
        }
        response = requests.post(url, json=payload, headers=self._get_headers())
        return response.json()

    def CreateLineageEvent(self, process_id, run_id, event_id, event_type, asset_name):
        """
        Create a new lineage event under a specific run.
        :param process_id: ID of the lineage process
        :param run_id: ID of the lineage run
        :param event_id: ID for the lineage event
        :param event_type: Type of the event (START, END, etc.)
        :param asset_name: Name of the asset involved in the event
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}/events/{event_id}"
        payload = {
            "eventType": event_type,
            "assetName": asset_name,
        }
        response = requests.post(url, json=payload, headers=self._get_headers())
        return response.json()

    def ShowLineageEvents(self, process_id, run_id, output_file_path=None):
        """
        Retrieve all lineage events under a specific run.
        :param process_id: ID of the lineage process
        :param run_id: ID of the lineage run
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}/lineageEvents"
        response = requests.get(url, headers=self._get_headers())
        json_text = response.json()
    
        # Save response to a JSON file
        if response.status_code == 200:
            if output_file_path:
                with open(output_file_path, "w") as f:
                    json.dump(json_text, f, indent=2)     
                    print(f"Lineage events saved to: {output_file_path}")

        return response.status_code, json_text


    def DeleteLineageEvent(self, process_id, run_id, event_id):
        """
        Delete a specific lineage event.
        :param process_id: ID of the lineage process
        :param run_id: ID of the lineage run
        :param event_id: ID of the lineage event to delete
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}/events/{event_id}"
        response = requests.delete(url, headers=self._get_headers())
        return response.status_code, response.text


