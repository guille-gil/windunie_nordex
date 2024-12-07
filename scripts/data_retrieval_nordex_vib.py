import os
import requests
import json
from datetime import datetime
from urllib import request
import xmltodict
import time
import logging
import urllib3

"""
This code is still in production. Not essential for the thesis, 
but good addition on top of a lambda function/cron expression
to ensure automatic retrieval.

Note: downloading raw files, not processed dataframes. 
The raw files need to follow a naming convention for further processing.
"""

# Disable insecure request warnings (for production, consider enabling verification)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

API_KEY = "TlgtY3VzdG9tZXJfKEEuTW9uc3Rvcm0vV2luZHVuaWUpOk5EWF9DTVNfVHJlbmRzXzJAMjI="

class CMSCrawler:
    def __init__(self):
        self.base_url = "https://cms-portal.nordex-online.com/weblog-datasrv"
        self.proxy = request.getproxies()
        self.bearer_token = self.authenticate()

    def authenticate(self):
        """Get the bearer token to authenticate the requests."""
        response_status_code = 429

        # Keep retrying on 429, 503, 504
        while response_status_code in (429, 503, 504):
            response = requests.get(
                f"{self.base_url}/auth/login",
                headers={"Authorization": f"Basic {API_KEY}"},
                proxies=self.proxy,
                verify=False
            )
            response_status_code = response.status_code
            sleep_time = 0
            if response_status_code == 429:
                sleep_time = int(response.headers.get('Retry-after', 60))
                logging.warning(f"Rate limited. Waiting {sleep_time} seconds before retrying.")
            elif response_status_code in (503, 504):
                sleep_time = 60
                logging.warning(f"Service unavailable (status {response_status_code}). Waiting {sleep_time} seconds before retrying.")
            time.sleep(sleep_time)

        # Determine response format (XML, JSON, or unknown)
        content_type = response.headers.get('Content-Type', '').lower()

        if 'xml' in content_type:
            # Attempt to parse as XML
            try:
                json_response = xmltodict.parse(response.text)
                # Convert dict from xmltodict to JSON string, then load as dict for easier handling
                json_data = json.loads(json.dumps(json_response))
                # Extract bearer token assuming XML structure: <accessToken><token>...</token></accessToken>
                auth_data = json_data.get('accessToken', {})
                bearer_token = auth_data.get('token')
                if not bearer_token:
                    logging.error("Login failed: 'token' not found in the XML response.")
                    logging.debug(f"Response content: {response.text}")
                    raise Exception("Login failed.")
                return bearer_token
            except xmltodict.expat.ExpatError as e:
                logging.error(f"Failed to parse XML response: {e}")
                logging.debug(f"Malformed XML or unexpected response: {response.text}")
                raise

        elif 'json' in content_type:
            # Attempt to parse as JSON
            try:
                auth_response = response.json()
                # Expecting a JSON with accessToken object: {"accessToken": {"token": "..."}}
                auth_data = auth_response.get('accessToken', {})
                bearer_token = auth_data.get('token')
                if not bearer_token:
                    logging.error("Login failed: 'token' not found in JSON response.")
                    logging.debug(f"Response content: {auth_response}")
                    raise Exception("Login failed.")
                return bearer_token
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON response: {e}")
                logging.debug(f"Malformed JSON response: {response.text}")
                raise

        else:
            # Neither XML nor JSON
            logging.error("Unexpected content type for authentication response.")
            logging.debug(f"Content-Type: {content_type}, Response: {response.text}")
            raise Exception("Login failed due to unexpected response format.")

    def get_systems(self):
        """Get the systems data."""
        response_status_code = 429
        while response_status_code in (429, 503, 504):
            response = requests.get(
                f"{self.base_url}/systems",
                headers={"Authorization": f"Bearer {self.bearer_token}"},
                proxies=self.proxy,
                verify=False
            )
            response_status_code = response.status_code
            sleep_time = 0
            if response_status_code == 429:
                sleep_time = int(response.headers.get('Retry-after', 60))
                logging.warning(f"Rate limited. Waiting {sleep_time} seconds before retrying.")
            elif response_status_code in (503, 504):
                sleep_time = 60
                logging.warning(f"Service unavailable (status {response_status_code}). Waiting {sleep_time} seconds before retrying.")
            time.sleep(sleep_time)

        content_type = response.headers.get('Content-Type', '').lower()
        if 'xml' in content_type:
            try:
                json_response = xmltodict.parse(response.text)
                json_data = json.loads(json.dumps(json_response))
                systems_data = json_data.get('systemItems', {})
                if not systems_data.get('systemItem'):
                    logging.error("No devices found in the XML response.")
                    raise Exception("No devices found.")
                return systems_data['systemItem']
            except xmltodict.expat.ExpatError as e:
                logging.error(f"Failed to parse systems XML response: {e}")
                logging.debug(f"Response content: {response.text}")
                raise
        elif 'json' in content_type:
            try:
                resp_json = response.json()
                # Adjust according to actual JSON structure if returned by the API
                systems_data = resp_json.get('systemItems', {})
                if not systems_data.get('systemItem'):
                    logging.error("No devices found in the JSON response.")
                    raise Exception("No devices found.")
                return systems_data['systemItem']
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse systems JSON response: {e}")
                logging.debug(f"Response content: {response.text}")
                raise
        else:
            logging.error("Unexpected content type for systems response.")
            logging.debug(f"Content-Type: {content_type}, Response: {response.text}")
            raise Exception("Failed to retrieve systems due to unexpected response format.")

    def get_device_files(self, system_item, start, end):
        """Get the raw files for a device."""
        response_status_code = 429
        while response_status_code in (429, 503, 504):
            response = requests.get(
                f"{self.base_url}/systems/{system_item['id']}/rawfiles",
                headers={"Authorization": f"Bearer {self.bearer_token}"},
                params={"from": int(start.timestamp()), "to": int(end.timestamp())},
                proxies=self.proxy,
                verify=False
            )
            response_status_code = response.status_code
            sleep_time = 0
            if response_status_code == 429:
                sleep_time = int(response.headers.get('Retry-after', 60))
                logging.warning(f"Rate limited. Waiting {sleep_time} seconds before retrying.")
            elif response_status_code in (503, 504):
                sleep_time = 60
                logging.warning(f"Service unavailable (status {response_status_code}). Waiting {sleep_time} seconds before retrying.")
            time.sleep(sleep_time)

        content_type = response.headers.get('Content-Type', '').lower()
        if 'xml' in content_type:
            try:
                json_response = xmltodict.parse(response.text)
                json_data = json.loads(json.dumps(json_response))
                return json_data.get('rawFileItems', {})
            except xmltodict.expat.ExpatError as e:
                logging.error(f"Failed to parse device files XML response: {e}")
                logging.debug(f"Response content: {response.text}")
                raise
        elif 'json' in content_type:
            try:
                resp_json = response.json()
                # Adjust keys based on actual JSON structure if returned by the API
                return resp_json.get('rawFileItems', {})
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse device files JSON response: {e}")
                logging.debug(f"Response content: {response.text}")
                raise
        else:
            logging.error("Unexpected content type for device files response.")
            logging.debug(f"Content-Type: {content_type}, Response: {response.text}")
            raise Exception("Failed to retrieve device files due to unexpected response format.")

    def resolve_file_name(self, system_item, raw_file_item, format):
        """Resolve the file name according to the documentation."""
        tstmp = datetime.fromtimestamp(int(raw_file_item['unixTime']))
        date_part = tstmp.strftime("%Y%m%d-%H%M%S")
        channel = raw_file_item['channel']
        method = raw_file_item['method']
        filter_value = raw_file_item['filter']

        file_title = f"{system_item['identity']}-{channel}.{method}_{date_part}"
        if int(filter_value) > -1:
            file_title += f"_filter{filter_value}"
        file_title += f".{format}"
        return file_title

    def download_raw_file(self, system_item, raw_file_item, format):
        """Download the raw file from the server and save it to the correct folder."""
        download_uri = self.resolve_download_uri(system_item, raw_file_item, format)
        response_status_code = 429
        while response_status_code in (429, 503, 504):
            response = requests.get(
                download_uri,
                headers={
                    "Authorization": f"Bearer {self.bearer_token}",
                    "Accept": "application/binary"
                },
                proxies=self.proxy,
                stream=True,
                verify=False
            )
            response_status_code = response.status_code
            sleep_time = 0
            if response_status_code == 429:
                sleep_time = int(response.headers.get('Retry-after', 60))
                logging.warning(f"Rate limited. Waiting {sleep_time} seconds before retrying.")
            elif response_status_code in (503, 504):
                sleep_time = 60
                logging.warning(f"Service unavailable (status {response_status_code}). Waiting {sleep_time} seconds before retrying.")
            time.sleep(sleep_time)

        if response_status_code != 200:
            logging.error(f"Failed to download file. HTTP {response_status_code}. Response: {response.text}")
            raise Exception(f"Failed to download file from {download_uri}")

        # Resolve file name
        file_name = self.resolve_file_name(system_item, raw_file_item, format)

        # Extract metadata to determine the component name
        # raw_file_item['metadata'] might not always exist or might not be dict - handle gracefully
        metadata = raw_file_item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        component_name = metadata.get('component', 'UnknownComponent')
        wind_turbine_name = system_item.get('identity', 'UnknownTurbine')

        # Define the folder path
        folder_path = os.path.join("raw", wind_turbine_name, component_name)

        # Ensure the folder exists
        os.makedirs(folder_path, exist_ok=True)

        # Define the full file path
        file_path = os.path.join(folder_path, file_name)

        # Save the file
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logging.info(f"Saved raw file to: {file_path}")

    def resolve_download_uri(self, system_item, raw_file_item, format):
        """Resolve the download URI according to the documentation."""
        tstmp = datetime.fromtimestamp(int(raw_file_item['unixTime']))
        date_part = tstmp.strftime("%Y%m%d-%H%M%S")
        filter_value = raw_file_item['filter']
        file_part = f"{raw_file_item['channel']}.{raw_file_item['method']}_{date_part}"
        if int(filter_value) > -1:
            file_part += f"_filter{filter_value}"
        file_part += f".{format}"
        return f"{self.base_url}/systems/{system_item['id']}/rawfiles/{file_part}"


def main(start, end):
    crawler = CMSCrawler()
    raw_systems_data = crawler.get_systems()

    systems_data = {}
    for item in raw_systems_data:
        # item is likely a dict from XML/JSON: ensure correct key extraction
        # Adjust keys if needed based on actual response structure
        # Example assumption: item = {"id": "...", "identity": "...", "name": "..."}
        device_id = item.get('id')
        identity = item.get('identity')
        name = item.get('name')
        if not all([device_id, identity, name]):
            continue  # skip incomplete data

        # Extract cleaned device name
        # This logic depends on the actual name format
        # If 'name' is something like "SomePrefix-DEN-TOL-N-89608"
        # and you want everything after 'SomePrefix-', adjust the splitting
        parts = name.split('-')
        # Adjust this logic as needed to correctly identify the cleaned device name
        # Below assumes that the first 3 parts are something you don't want, and you keep the rest
        if len(parts) > 3:
            cleaned_device_name = "-".join(parts[3:])
        else:
            cleaned_device_name = name

        if cleaned_device_name not in systems_data:
            systems_data[cleaned_device_name] = []
        systems_data[cleaned_device_name].append((device_id, identity))

    logging.info(systems_data)

    # List of device_ids for DenTol and Bommelerwaard
    # Adjust based on your actual selection criteria
    selected_device_ids = ['7592']

    for system_name, devices in systems_data.items():
        for device_id, device_name in devices:
            if device_id not in selected_device_ids:
                continue
            logging.info(f"Processing device ID: {device_id} - Device Name: {device_name}")
            raw_files_data = crawler.get_device_files({'id': device_id}, start, end)
            if not raw_files_data:
                logging.info(f"No raw files data found for device {device_id}.")
                continue

            # raw_files_data might be dict; ensure 'rawFileItem' exists
            raw_file_items = raw_files_data.get('rawFileItem', [])
            # If rawFileItem is a single dict, wrap it in a list
            if isinstance(raw_file_items, dict):
                raw_file_items = [raw_file_items]

            for file_item in raw_file_items:
                crawler.download_raw_file({'id': device_id, 'identity': device_name}, file_item, "txt")


if __name__ == "__main__":
    start = datetime(2024, 11, 30, 0, 0)
    end = datetime(2024, 11, 30, 2, 0)
    main(start, end)