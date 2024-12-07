import pandas as pd
import requests
import json
from datetime import datetime
from urllib import request
import xmltodict
import re
import time

API_KEY = "TlgtY3VzdG9tZXJfKEEuTW9yb24vV2luZHVuaWUpOk5EWF9DTVNfVHJlbmRzXzJAMjI="


# Delete for production
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CMSCrawler:
    def __init__(self):
        self.base_url = "https://cms-portal.nordex-online.com/weblog-datasrv"
        self.proxy = request.getproxies()
        self.bearer_token = self.authenticate()

    def authenticate(self):
        """ get the bearer token to authenticate the requests """
        """response = requests.get(
            f"{self.base_url}/auth/login",
            headers={
                "Authorization": f"Basic {API_KEY}"
            },
            proxies=self.proxy, verify=False
        )
        if response.status_code in (429, 503, 504):
            raise Exception("Note: Handle the (429, 503, 504) status codes.")"""

        response_status_code = 429
        while response_status_code in (429, 503, 504):
            response = requests.get(
                f"{self.base_url}/auth/login",
                headers={
                    "Authorization": f"Basic {API_KEY}"
                },
                proxies=self.proxy, verify=False
            )
            response_status_code = response.status_code
            sleep_time = 0
            if response_status_code == 429:
                sleep_time = int(response.headers.get('Retry-after'))
            elif response_status_code in (503, 504):
                sleep_time = 60
            print(f"Too many requests. Waiting {sleep_time} seconds...")
            time.sleep(sleep_time)

        # parse the response to json
        json_response = xmltodict.parse(response.text)
        json_data = json.dumps(json_response, indent=2)
        auth_data = json.loads(json_data).get('accessToken', {})

        # get the bearer token
        if not auth_data.get('token'):
            raise Exception("Login failed.")
        bearer_token = auth_data.get('token')

        return bearer_token

    def get_systems(self):
        """ get the systems data """
        """response = requests.get(
            f"{self.base_url}/systems",
            headers={"Authorization": f"Bearer {self.bearer_token}"},
            proxies=self.proxy,
            verify=False
        )
        if response.status_code in (429, 503, 504):
            raise Exception("Note: Handle the (429, 503, 504) status codes.")"""

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
                sleep_time = int(response.headers.get('Retry-after'))
            elif response_status_code in (503, 504):
                sleep_time = 60
            print(f"Too many requests. Waiting {sleep_time} seconds...")
            time.sleep(sleep_time)

        # parse the response to json
        json_response = xmltodict.parse(response.text)
        json_data = json.dumps(json_response, indent=2)
        systems_data = json.loads(json_data).get('systemItems', {})

        if not systems_data.get('systemItem'):
            raise Exception("No devices found.")
        raw_systems_data = systems_data['systemItem']

        return raw_systems_data

    def get_device_files(self, system_item, start, end):
        """ get the raw files for a device """

        """response = requests.get(
            headers={"Authorization": f"Bearer {self.bearer_token}"},
            params={"from": int(start.timestamp()), "to": int(end.timestamp())},
            proxies=self.proxy,
            verify=False
        )
        if response.status_code in (429, 503, 504):
            raise Exception("Note: Handle the (429, 503, 504) status codes.")"""

        response_status_code = 429
        while response_status_code in (429, 503, 504):
            response = requests.get(
                 f"{self.base_url}/systems/{system_item['id']}/rawfiles",
                headers={
                    "Authorization": f"Bearer {self.bearer_token}"},
                params={"from": int(start.timestamp()), "to": int(end.timestamp())},
                proxies=self.proxy, verify=False
            )
            response_status_code = response.status_code
            sleep_time = 0
            if response_status_code == 429:
                sleep_time = int(response.headers.get('Retry-after'))
            elif response_status_code in (503, 504):
                sleep_time = 60
            print(f"Too many requests. Waiting {sleep_time} seconds...")
            time.sleep(sleep_time)

        # parse the response to json
        json_response = xmltodict.parse(response.text)
        json_data = json.dumps(json_response, indent=2)
        raw_files_data = json.loads(json_data).get('rawFileItems', {})

        if raw_files_data is not None:
            found_items = len(raw_files_data.get('rawFileItem', []))
            print(f"Found {found_items} raw files in the server for this device")

        return raw_files_data

    def resolve_file_name(self, system_item, raw_file_item, format):
        """ resolve the file name according to the documentation """
        tstmp = datetime.fromtimestamp(int(raw_file_item['unixTime']))
        date_part = tstmp.strftime("%Y%m%d-%H%M%S")

        channel = raw_file_item['channel']
        method = raw_file_item['method']
        filter = raw_file_item['filter']
        file_title = f"{system_item['identity']}-{channel}.{method}_{date_part}"
        if int(filter) > -1:
            file_title += f"_filter{filter}"
        file_title += f".{format}"

        return file_title, system_item['identity']

    def resolve_download_uri(self, system_item, raw_file_item, format):
        """ resolve the download uri according to the documentation """
        # tstmp = datetime.fromtimestamp(int(raw_file_item['unixTime']))


        from datetime import timezone
        tstmp = datetime.fromtimestamp(int(raw_file_item['unixTime']), tz=timezone.utc)
        date_part = tstmp.strftime("%Y%m%d-%H%M%S")
        filter = raw_file_item['filter']
        file_part = f"{raw_file_item['channel']}.{raw_file_item['method']}_{date_part}"
        if int(filter) > -1:
            file_part += f"_filter{filter}"
        file_part += f".{format}"

        return f"{self.base_url}/systems/{system_item['id']}/rawfiles/{file_part}"

    @staticmethod
    def parse_file_as_dataframe(file_path, group_name, asset_name):
        """ parse the server file response as a dataframe """
        # NOTE: feel free to addapt/improve this function

        with open(file_path, 'r') as file:
            content = file.read()

        # Split the content into sections
        sections = re.split(r'\[([\w]+):\d+\]', content)[1:]

        data = []
        for i in range(0, len(sections), 4):
            section_content = sections[i + 1] if i + 1 < len(sections) else ""

            # Extract metadata
            metadata = {}
            for line in section_content.split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    metadata[key.strip()] = value.strip()

            # Extract tag_name (sensor)
            tag_name = metadata.get('szLabel', 'Unknown')

            # Parse the data
            data_section = sections[i + 3] if i + 3 < len(sections) else ""
            data_section = data_section.replace('#--finish--\n\n', '')

            if data_section:
                rows = []
                for line in data_section.strip().split('\n'):
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        timestamp, value = parts[:2]
                        rows.append([float(timestamp), float(value)])
                df = pd.DataFrame(rows, columns=['timestamp', 'value'])

                df['tag_name'] = tag_name
                df['group_name'] = group_name
                df['asset_name'] = asset_name

                start_time = int(metadata.get('starttime', 0))
                df['timestamp'] = pd.to_datetime(start_time, unit='s') + pd.to_timedelta(df['timestamp'], unit='s')

                data.append(df)

        # Combine all data into a single DataFrame
        final_df = pd.DataFrame()
        if data:
            final_df = pd.concat(data, ignore_index=True)

        return final_df

    def download_raw_file(self, system_item, raw_file_item, format, system_name, device_name):
        """ download the raw file from the server """
        # working example to download: ({'id': '3985'}, {'unixTime': '1732861993', 'channel': '7', 'method': '3', 'filter': '-1', 'hash': 'NA'}, 'txt')
        download_uri = self.resolve_download_uri(system_item, raw_file_item, format)
        """response = requests.get(
            download_uri,
            headers={
                "Authorization": f"Bearer {self.bearer_token}",
                "Accept": "application/binary"
            },
            proxies=self.proxy,
            stream=True,
            verify=False
        )
        if response.status_code in (429, 503, 504):
            raise Exception("Note: Handle the (429, 503, 504) status codes.")"""

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
                sleep_time = int(response.headers.get('Retry-after'))
            elif response_status_code in (503, 504):
                sleep_time = 60
            print(f"Too many requests. Waiting {sleep_time} seconds...")
            time.sleep(sleep_time)


        with open('temp.txt', 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        df = self.parse_file_as_dataframe('temp.txt', system_name, device_name)

        return df


def main(start, end):
    crawler = CMSCrawler()
    raw_systems_data = crawler.get_systems()

    systems_data = {}
    for item in raw_systems_data:
        device_id, identity, name = item.values()
        cleaned_device_name = "-".join(name.split('-')[3:])
        if cleaned_device_name not in systems_data:
            systems_data[cleaned_device_name] = []
        systems_data[cleaned_device_name].append((device_id, identity))

    print(systems_data)

    dataframes = []
    for system_name, devices in systems_data.items():
        for device_id, device_name in devices:
            raw_files_data = crawler.get_device_files({'id': device_id}, start, end)
            if not raw_files_data:
                continue
            raw_files_data = raw_files_data['rawFileItem']

            for file in raw_files_data:
                _, asset_name = crawler.resolve_file_name({'identity': f"Device_{device_id}"}, file, "txt")
                df = crawler.download_raw_file({'id': device_id}, file, "txt", system_name, asset_name)
                if not df.empty:
                    dataframes.append(df)

    return dataframes


if __name__ == "__main__":
    start = datetime(2024, 11, 30, 0, 0)
    end = datetime(2024, 11, 30, 3, 0)

    """ end = datetime.now()
    start = end - datetime.timedelta(days=1) """

    dataframes = main(start, end)

    print(dataframes)