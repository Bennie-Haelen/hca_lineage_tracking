import json
import requests
from google.auth import default
from google.auth.transport.requests import Request

class Lineage:

    def __init__(self, project_id, location, base_url):
        """
        Initialize the Lineage class.
        :param project_id: GCP Project ID
        :param location: GCP Location/Region
        :param base_url: Base URL for Lineage API
        """
        self.project_id = project_id
        self.location = location
        self.base_url = base_url.rstrip('/')



    def _get_access_token(self):
        """
        Retrieve an access token using the default credentials.
        :return: A valid access token as a string
        """
        try:
            # Get default credentials and refresh them to ensure the token is valid
            credentials, _ = default()
            credentials.refresh(Request())  # Refresh credentials to get the token
            return credentials.token
        except Exception as e:
            print(f"Error obtaining access token: {e}")
            raise


        
    def _get_headers(self):
        """
        Helper to create headers for HTTP requests.
        Replace this with an actual Authorization method.
        """
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._get_access_token()}"
        }


    def get_process(self, process_id):
        """
        Retrieve a specific lineage process.
        :param process_id: ID of the lineage process
        :return: Tuple containing HTTP status code and response JSON or error message
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            return response.status_code, response.json()
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return response.status_code, {"error": str(http_err)}
        
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return 500, {"error": str(req_err)}
        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return 500, {"error": "An unexpected error occurred"}

    

    def get_process_run(self, process_id, run_id):
        """
        Retrieve a specific lineage run for a process.
        :param process_id: ID of the lineage process
        :param run_id: ID of the lineage run
        :return: Tuple containing HTTP status code and response JSON or error message
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            return response.status_code, response.json()
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return response.status_code, {"error": str(http_err)}
        
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return 500, {"error": str(req_err)}
        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return 500, {"error": "An unexpected error occurred"}
        


    def search_target_lineage(self, dataset_id, table_id):
        
        search_url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}:searchLinks"

        # Construct the source entity id
        target_entity_id = f"bigquery:{self.project_id}.{dataset_id}.{table_id}"

        # Construct the request body with the fully qualified name
        request_body = {
            "pageSize": 100,
            "target": { 
                "fullyQualifiedName": target_entity_id,
            }
        }

        try:
            response = requests.post(search_url, json=request_body, headers=self._get_headers())
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

            response_obj = response.json()
            # We want to return the links
            if response_obj and "links" in response_obj:
                links = response.json()["links"]
                return response.status_code, links
            else:
                return response.status_code, None

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return response.status_code, {"error": str(http_err)}
        
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return 500, {"error": str(req_err)}
        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return 500, {"error": "An unexpected error occurred"}
        

    def get_source_tables_for_table(self, dataset_id, table_id):
        """
        Retrieve the source tables for a given target table.
        :param dataset_id: The dataset ID containing the target table
        :param table_id: The target table ID
        :return: Tuple containing HTTP status code and a list of source table fully qualified names or an error message
        """
        try:
            # Get the target lineage for the specified table
            status_code, target_lineages = self.search_target_lineage(dataset_id, table_id)

            # Check if the lineage retrieval was successful
            if status_code != 200:
                return status_code, {"error": f"Failed to retrieve target lineage: HTTP {status_code}"}

            # Extract the source tables from the lineage data
            source_tables = [
                item["source"]["fullyQualifiedName"].split(":", 1)[1]  # Remove the prefix (e.g., 'bigquery:')
                for item in target_lineages
            ]

            return status_code, source_tables

        except KeyError as e:
            print(f"Key error while processing lineage data: {e}")
            return 500, {"error": f"Key error while processing lineage data: {e}"}

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return 500, {"error": "An unexpected error occurred"}



    def get_full_source_description_for_table(self, dataset_id, table_id):
        """
        Retrieve a detailed description of the source tables and process details for a given table.
        :param dataset_id: The dataset ID containing the target table
        :param table_id: The target table ID
        :return: A formatted string with the source table details and process information
        """
        try:
            # Step 1: Get source tables
            status_code, source_tables = self.get_source_tables_for_table(dataset_id, table_id)
            if status_code != 200 or not source_tables:
                return f"Failed to retrieve source tables for table: {dataset_id}.{table_id}"

            # Step 2: Retrieve lineage links
            status_code, target_lineages = self.search_target_lineage(dataset_id, table_id)
            if status_code != 200 or not target_lineages:
                return f"Failed to retrieve lineage information for table: {dataset_id}.{table_id}"

            link_names = [item["name"] for item in target_lineages]

            # Step 3: Get process ID from the lineage links
            process_id = next(
                (self.retrieve_process_for_link(link_name).split('/')[-1] for link_name in link_names if self.retrieve_process_for_link(link_name)),
                None
            )
            if not process_id:
                return f"No process information found for table: {dataset_id}.{table_id}"

            # Step 4: Retrieve process details
            status_code, process_details = self.get_process(process_id)
            if status_code != 200 or not process_details:
                return f"Failed to retrieve process details for process ID: {process_id}"

            process_name = process_details.get("displayName", "Unknown")
            big_query_job_id = process_details.get("attributes", {}).get("bigquery_job_id", "Unknown")

            # Step 5: Format and return the result
            result = (
                f"The table `{dataset_id}.{table_id}` is sourced from the following tables: {', '.join(source_tables)}\n"
                f"The transformation was performed by the following process:\n"
                f"    Process ID: {process_id}\n"
                f"    Process Name: {process_name}\n"
                f"    BigQuery Job ID: {big_query_job_id}"
            )
            return result

        except Exception as e:
            print(f"An error occurred while generating the full source description: {e}")
            return "An unexpected error occurred. Please check the logs for details."


    def create_lineage_process(self, display_name, environment="Lakehouse"):
        """
        Create a new lineage process.
        :param process_id: ID for the lineage process
        :param display_name: Human-readable name for the process
        :return: Response JSON or None in case of errors
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes"
        payload = {"displayName": display_name}


        # Construct the request body
        request_body = {
            "displayName": display_name,
            "attributes":{
                "environment": environment
            }
        }

        try:
            response = requests.post(url, json=request_body, headers=self._get_headers())
            response.raise_for_status()
            return response.status_code, response.json()

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred while creating the lineage process: {http_err}")
            return response.status_code, None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred while creating the lineage process: {req_err}")
            return 500, None
        except Exception as e:
            print(f"An unexpected error occurred while creating the lineage process: {e}")
            return 500, None
        

    def create_lineage_run(self, process_id, display_name, start_time, end_time, end_state="COMPLETED", environment="Lakehouse"):
        """
        Create a new lineage run under a specific process.
        :param process_id: ID of the lineage process
        :param run_id: ID for the lineage run
        :param start_time: Start time of the run
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs"

        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # Construct the request body
        request_body = {
            "displayName": display_name,
            "attributes":{
                "environment": environment
            },
            "startTime": start_time_str,
            "endTime": end_time_str,
            "state": end_state
        }

        try:
            response = requests.post(url, json=request_body, headers=self._get_headers())
            response.raise_for_status()
            return response.status_code, response.json()

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred while creating the lineage run: {http_err}")
            return response.status_code, None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred while creating the lineage run: {req_err}")
            return 500, None
        except Exception as e:
            print(f"An unexpected error occurred while creating the lineage run: {e}")
            return 500, None



    def create_lineage_event(self, process_id, run_id, dataset_id, sources, target, start_time, end_time):

        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}/lineageEvents"

        event_links = []
        for source in sources:
            event_links.append({
                "source": { "fullyQualifiedName": f"bigquery:{self.project_id}.{dataset_id}.{source}" }, 
                "target": { "fullyQualifiedName": f"bigquery:{self.project_id}.{dataset_id}.{target}" } 
                })

        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # Construct the request body
        request_body = {
            "links": event_links,
            "startTime": start_time_str,
            "endTime": end_time_str
        }

        print("Request Body: ", request_body)

        try:
            response = requests.post(url, json=request_body, headers=self._get_headers())
            response.raise_for_status()
            return response.status_code, response.json()

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred while creating the lineage event: {http_err}")
            return response.status_code, None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred while creating the lineage event: {req_err}")
            return 500, None
        except Exception as e:
            print(f"An unexpected error occurred while creating the lineage event: {e}")
            return 500, None




    def retrieve_process_for_link(self, link_name):
        """
        Retrieve the process associated with a specific lineage link.
        :param link_name: The name of the lineage link
        :return: Process name or None if not found
        """
        search_url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}:batchSearchLinkProcesses"

        # Construct the request body
        request_body = {
            "links": [link_name],
            "pageSize": 100,
        }

        try:
            response = requests.post(search_url, json=request_body, headers=self._get_headers())
            response.raise_for_status()
            response_json = response.json()

            # Check if processLinks exists and contains the process
            process_links = response_json.get("processLinks", [])
            if process_links:
                process = process_links[0].get("process")
                return process
            else:
                return None

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    


    

    def search_target_lineage(self, dataset_id, table_id):
        
        search_url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}:searchLinks"

        # Construct the source entity id
        source_entity_id = f"bigquery:{self.project_id}.{dataset_id}.{table_id}"

        # Construct the request body
        request_body = {
            "pageSize": 100,
            "target": { 
                "fullyQualifiedName": source_entity_id,
            }
        }

        response = requests.post(search_url, json=request_body, headers=self._get_headers())
        # json_text = response.json()["links"]
        json_text = response.json()
        print(f"Search target Lineage: {json_text}")
        return response.status_code, json_text



    def search_source_lineage(self, dataset_id, table_id):
        
        search_url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}:searchLinks"

        # Construct the source entity id
        source_entity_id = f"bigquery:{self.project_id}.{dataset_id}.{table_id}"

        # Construct the request body
        request_body = {
            "pageSize": 100,
            "source": { 
                "fullyQualifiedName": source_entity_id,
            }
        }

        response = requests.post(search_url, json=request_body, headers=self._get_headers())
        # json_text = response.json()["links"]
        json_text = response.json()
        print(f"Search Source Lineage: {json_text}")
        return response.status_code, json_text




    def show_lineage_events(self, process_id, run_id, output_file_path=None):
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


    def delete_lineage_event(self, process_id, run_id, lineage_event_id):
        """
        Delete a specific lineage event.
        :param process_id: ID of the lineage process
        :param run_id: ID of the lineage run
        :param event_id: ID of the lineage event to delete
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs/{run_id}/lineageEvents/{lineage_event_id}"

        try:
            response = requests.delete(url, headers=self._get_headers())
            response.raise_for_status()
            response_json = response.json()
            return response.status_code, response_json

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred during delete_lineage_event: {http_err}")
            return response.status_code, None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred during delete_lineage_event:{req_err}")
            return 500, None
        except Exception as e:
            print(f"An unexpected error occurred during delete_lineage_event: {e}")
            return 500, None            

    


    def retrieve_all_runs_for_process(self, process_id):
        """
        Retrieve all lineage runs for a specific process.
        :param process_id: ID of the lineage process
        """
        url = f"{self.base_url}/projects/{self.project_id}/locations/{self.location}/processes/{process_id}/runs"

        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            response_json = response.json()
            return response.status_code, response_json
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred during retrieve_all_runs_for_process: {http_err}")
            return response.status_code, None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred during retrieve_all_runs_for_process:{req_err}")
            return 500, None
        except Exception as e:
            print(f"An unexpected error occurred during retrieve_all_runs_for_process: {e}")
            return 500, None            
        



    def get_link_names_from_links(self, links):
        """
        Extracts the 'name' field from a list of link objects.

        :param links: list
            A list of link dictionaries, where each dictionary contains a 'name' key.
        
        :return: list
            A list of 'name' values extracted from the input links.
            Returns an empty list if the input is None or empty.
        """
        # Use a list comprehension to extract 'name' fields from links if the input is not None or empty
        return [link["name"] for link in links] if links else []



    def search_lineage_for_partial_fqdn(self, dataset_id, table_id, partial_fqdn, search_target=True):
        """
        Search for target lineage links matching a partial Fully Qualified Domain Name (FQDN).

        :param dataset_id: str
            The dataset ID containing the target table.
        :param table_id: str
            The target table ID.
        :param partial_fqdn: str
            A partial string to search for in the Fully Qualified Domain Names (FQDNs) of the lineage links.

        :return: tuple
            A tuple containing:
            - HTTP status code (200 if successful).
            - A list of matching link names where the `partial_fqdn` was found in the source's FQDN.
        
        :raises:
            No exceptions are raised explicitly, but any exceptions in `search_target_lineage` will propagate.
        """
        try:
            # Step 1: Retrieve all target lineage links for the given table
            if search_target:
                status_code, links = self.search_target_lineage(dataset_id, table_id)
            else:
                status_code, links = self.search_source_lineage(dataset_id, table_id)

            if status_code != 200:
                print(f"Failed to retrieve target lineage for {dataset_id}.{table_id}. HTTP Status: {status_code}")
                return status_code, []

            # Debug: Print the links for inspection
            print("Retrieved lineage links:")
            print(json.dumps(links, indent=2))

            matching_link_names = []
            # Step 2: Search for FQDNs containing the partial FQDN
            print(f"Looking for partial FQDN: {partial_fqdn}")
            
            for link in links.get("links", []):
                if search_target:
                    fqdn = link["source"].get("fullyQualifiedName", "")
                else:
                    fqdn = link["target"].get("fullyQualifiedName", "")
                print(f"Checking FQDN: {fqdn}")
                if partial_fqdn in fqdn:
                    print(f"Found matching FQDN: {fqdn}, link name: {link['name']}")
                    matching_link_names.append(link["name"])
                else:
                    print(f"No Match for partial_fqdn: {partial_fqdn} in FQDN: {fqdn}")

            # Step 3: Return the results
            return 200, matching_link_names

        except KeyError as e:
            print(f"KeyError occurred while processing lineage data: {e}")
            return 500, []
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return 500, []



    def delete_lineage_events_for_link_names(self, links):
        """
        Delete all lineage events associated with a list of lineage link names.

        :param links: list
            A list of lineage link names.

        :raises:
            Any exceptions in the process will propagate unless explicitly handled.
        """
        print(f"in delete, passed in links: {links}, total links: {len(links)}")
        try:
            for link in links:
                print(f"Processing link: {link}")
                # Retrieve the full process name associated with the link
                full_process_name = self.retrieve_process_for_link(link)
                process_name = full_process_name.split('/')[-1]
                print(f"Processing link: {link}, process: {process_name}")

                # Retrieve all runs for the process
                status_code, runs_response = self.retrieve_all_runs_for_process(process_name)
                print(f"After runs response: {runs_response}")
                runs = runs_response.get("runs", [])
                if not runs:
                    print(f"No runs found for process: {process_name}")
                    continue

                for run in runs:
                    run_name = run["name"].split('/')[-1]
                    print(f"  Processing run: {run_name}")

                    # Retrieve lineage events for the run
                    status_code, lineage_events_response = self.show_lineage_events(process_name, run_name)
                    if status_code != 200 or not lineage_events_response:
                        print(f"  Failed to retrieve lineage events for run: {run_name} (HTTP {status_code})")
                        continue

                    lineage_events = lineage_events_response.get("lineageEvents", [])
                    if not lineage_events:
                        print(f"  No lineage events found for run: {run_name}")
                        continue

                    for lineage_event in lineage_events:
                        # Extract the lineage event ID
                        lineage_event_id = lineage_event["name"].split('/')[-1]
                        print(f"    Deleting lineage event: {lineage_event_id}")

                        # Delete the lineage event
                        self.delete_lineage_event(process_name, run_name, lineage_event_id)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")



    def get_resource_type(self, fully_qualified_name):
        # Extract the first part of the name
        if fully_qualified_name.startswith("bigquery:"):
            return "BigQuery Table"
        elif fully_qualified_name.startswith("gcs:"):
            return "GCS Bucket"
        else:
            return "Unknown Resource"



    def delete_lineage_from_target_dictionary(self, delete_targets):
        """
        Deletes all lineage events specified in the target dictionary.

        :param delete_targets: list
            A list of dictionaries where each dictionary specifies:
            - `table_id` (str): Fully qualified table ID in the format "dataset_id.table_id".
            - `gcs_buckets` (list): List of GCS bucket names to match for deletion.
            - `source_tables` (list): List of BigQuery source table names to match for deletion.
        """
        try:
            # Iterate through each target in the delete_targets list
            for target in delete_targets:
                # Extract target details
                table_id = target.get("table_id")
                gcs_buckets = target.get("gcs_buckets", [])
                source_tables = target.get("source_tables", [])

                # Parse dataset and table ID
                dataset_id, table_name = table_id.split(".")
                print(f"Processing target: dataset_id={dataset_id}, table_id={table_name}")
                print(f"\tGCS Buckets: {gcs_buckets}")
                print(f"\tSource Tables: {source_tables}")

                # Step 1: Retrieve source and target lineage links
                status_code, lineage_links_target_response = self.search_target_lineage(dataset_id, table_name)
                status_code, lineage_links_source_response = self.search_source_lineage(dataset_id, table_name)

                # Check for successful API response
                if status_code != 200:
                    print(f"Failed to retrieve lineage links for {table_name}. HTTP Status: {status_code}")
                    continue

                # Prepare a list to store links to be deleted
                links_to_be_deleted = []

                for lineage_links_response, direction in [(lineage_links_target_response, "target"), (lineage_links_source_response, "source")]:

                    # Extract lineage links from the response
                    lineage_links = lineage_links_response.get("links", [])
                    print(f"Retrieved {len(lineage_links)} lineage links")


                    # Step 2: Filter lineage links based on GCS buckets or source tables
                    for link in lineage_links:
                        # Extract the fully qualified name of the source
                        if direction == "source":
                            location = link['target']['fullyQualifiedName']
                            resource_type = self.get_resource_type(location)
                        else:
                            location = link['source']['fullyQualifiedName']
                            resource_type = self.get_resource_type(location)
                        print(f"Resource type: {resource_type}, Location: {location}")

                        # Match and collect links for GCS buckets
                        if resource_type == "GCS Bucket":
                            if any(bucket in location for bucket in gcs_buckets):
                                links_to_be_deleted.append(link)

                        # Match and collect links for BigQuery tables
                        elif resource_type == "BigQuery Table":
                            print(f"Checking for BigQuery Table, source_tables: {source_tables}")
                            if any(table in location for table in source_tables):
                                links_to_be_deleted.append(link)

                        # Unsupported resource types
                        else:
                            print(f"Unsupported resource type: {resource_type}")
                            continue

                # Extract the names of the links to be deleted
                link_names = self.get_link_names_from_links(links_to_be_deleted)

                # Step 3: Delete the identified lineage links
                if link_names:
                    print(f"Found {len(link_names)} lineage links to delete")
                    self.delete_lineage_events_for_link_names(link_names)
                else:
                    print(f"No lineage links to delete for table {dataset_id}.{table_name}")

        except Exception as e:
            print(f"An unexpected error occurred while deleting lineage: {e}")
