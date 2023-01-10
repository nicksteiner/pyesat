import os
import sys
import json
import pprint
from typing import List, Dict
from pathlib import Path
import urllib
import netrc
from datetime import datetime
import requests
import configparser
from requests import Session

# Generate a NASA Earthdata Login Token

config_file = Path(__file__).parent / 'config.ini'  # same directory as the the source

remote_hostname = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication


def get_credentials() -> Dict:
    config_parser = configparser.ConfigParser()
    try:
        config_parser.read_file(open(config_file, 'r'))
        assert remote_hostname in config_parser
    except:
        raise Exception('NASA Earthdata credentials not found, please run: write_earthdata_credentials.py')
    return dict(config_parser[remote_hostname])


class CMRClient:
    """
    A Python API for interacting with the NASA Common Metadata Repository (CMR).

    Attributes:
    - base_url (str): The base URL for the CMR API.
    """

    def __init__(self, provider='LPCLOUD', project='ECOSTRESS'):
        self.base_url = "https://cmr.earthdata.nasa.gov/search"
        self.auth = get_credentials()
        self.provider = provider
        self.project = project
        self.headers = {
            'Authorization': f'Bearer {self.auth["token"]}',
            'Accept': 'application/json'
        }

    def get_collections(self, verbose=True):
        token = self.auth['token']
        url = f'{self.base_url}/{"collections"}'
        response = requests.get(url,
                                params={
                                    'cloud_hosted': 'True',
                                    'has_granules': 'True',
                                    'provider': self.provider,
                                    'project': self.project,
                                    'page_size': 100
                                },
                                headers=self.headers
                                )
        try:
            assert response.status_code == 200
        except:
            error_ = json.loads(response.text)
            raise Exception(f"{error_['error']}:{error_['error_description']}")

        print(f"CRM-HITS: {response.headers['cmr-hits']}")
        content = response.json()
        collections = content['feed']['entry']
        for collection in collections:
            print(f'{collection["archive_center"]} | {collection["dataset_id"]} | {collection["id"]}')

    def search_granules(self, bbox, date_range, collection_id='C2076090826-LPCLOUD', verbose=True):
        """
        Search the CMR for granules.

        Here are some as of Dec. 2022:
        LP DAAC | ECOSTRESS Tiled Land Surface Temperature and Emissivity Instantaneous L2 Global 70 m V002 | C2076090826-LPCLOUD
        LP DAAC | ECOSTRESS Swath Geolocation Instantaneous L1B Global 70 m V002 | C2076087338-LPCLOUD
        LP DAAC | ECOSTRESS Swath Top of Atmosphere Calibrated Radiance Instantaneous L1B Global 70 m V002 | C2076116385-LPCLOUD
        LP DAAC | ECOSTRESS Swath Cloud Mask Instantaneous L2 Global 70 m V002 | C2076115306-LPCLOUD
        LP DAAC | ECOSTRESS Swath Land Surface Temperature and Emissivity Instantaneous L2 Global 70 m V002 | C2076114664-LPCLOUD

        Parameters:
        - params (dict): A dictionary of query parameters to use in the search.
          For a full list of available parameters and their meanings, see the
          CMR API documentation:
          https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#

        Returns:
        - dict: A dictionary of search results, in the same format as the JSON
          response returned by the CMR API.
        """
        url = f'{self.base_url}/{"granules"}'
        token = self.auth['token']
        params = {
            'concept_id': collection_id,
            'temporal': date_range,
            'bounding_box': bbox,
            'page_size': 2000
        }
        response = requests.get(url, params=params, headers=self.headers)
        try:
            assert response.status_code == 200
        except:
            error_ = json.loads(response.text)
            raise Exception(f"{error_['error']}:{error_['error_description']}")

        if verbose:
            print(f"{self.project}|{self.provider}|{collection_id} granules: {response.headers['CMR-Hits']}")

        response.raise_for_status()

        granules = response.json()['feed']['entry']

        granules_ = []
        for granule in granules:
            if verbose:
                print(f'{granule["data_center"]} | {granule["dataset_id"]} | {granule["id"]}')
            #https_urls = [l['href'] for l in granule['links'] if 'https' in l['href'] and '.tif' in l['href']]
            #s3_urls = [l['href'] for l in granule['links'] if 's3' in l['href'] and '.tif' in l['href']]
            #granule['s3'] = s3_urls
            #granule['https'] = https_urls

            granules_.append(Granule(granule))

        return granules_
class Granule:
    # Class to contain information from the CRM entry json object.

    """
    write function to put these fields into the Granule object parameters with the same names
    'producer_granule_id: ECOv002_L2T_LSTE_24972_017_10SGD_20221201T044006_0710_01'
    'time_start: 2022-12-01T04:40:06.140Z'
    'updated: 2022-12-10T16:07:24.091Z'
    ("orbit_calculated_spatial_domains: [{'start_orbit_number': '24972', "
     "'stop_orbit_number': '24972'}]")
    ('dataset_id: ECOSTRESS Tiled Land Surface Temperature and Emissivity '
     'Instantaneous L2 Global 70 m V002')
    'data_center: LPCLOUD'
    'title: ECOv002_L2T_LSTE_24972_017_10SGD_20221201T044006_0710_01'
    'coordinate_system: GEODETIC'
    'day_night_flag: NIGHT'
    'time_end: 2022-12-01T04:40:58.110Z'
    'id: G2562621237-LPCLOUD'
    'original_format: ECHO10'
    'granule_size: 3.79172'
    'browse_flag: True'
    'collection_concept_id: C2076090826-LPCLOUD'
    'online_access_flag: True'
    """
    _dt_parser = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, granule, verbose=True):
        self.id = granule['id']
        self.dataset_id = granule['dataset_id']
        self.data_center = granule['data_center']
        self.time_start = datetime.strptime(granule['time_start'], self._dt_parser)
        self.time_end = datetime.strptime(granule['time_end'], self._dt_parser)
        self.collection_concept_id = granule['collection_concept_id']
        self.producer_granule_id = granule['producer_granule_id']
        self.browse_flag = bool(granule['browse_flag'])
        self.online_access_flag = bool(granule['online_access_flag'])
        self.original_format = granule['original_format']
        self.coordinate_system = granule['coordinate_system']
        self.day_night_flag = granule['day_night_flag']
        self.title = granule['title']
        self.updated = granule['updated']
        self.granule_size = granule['granule_size']
        self.orbit_calculated_spatial_domains = granule['orbit_calculated_spatial_domains'][0]
        self.start_orbit_number = int(self.orbit_calculated_spatial_domains['start_orbit_number'])
        self.stop_orbit_number = int(self.orbit_calculated_spatial_domains['stop_orbit_number'])
        self.boxes = granule['boxes'][0]
        "boxes: ['34.207188 -120.828926 35.223133 -119.598442']"
        _bounds = [float(i) for i in self.boxes.split(' ')]
        self.bounds = [_bounds[1], _bounds[0], _bounds[3], _bounds[2]]
        self.links = Links(granule['links'])
        self.s3 = self.links.get_s3()
        self.https = self.links.get_https()
        if verbose:
            print(f'Granule: {self.id}: {self.dataset_id}: {self.time_start} - {self.time_end}')

    def __repr__(self):
        return f'{self.data_center} | {self.dataset_id} | {self.id}'

    def get_s3(self):
        return self.s3

    def get_https(self):
        return self.https

    def get_file_name(self):
        return self.file_name

    def get_json(self):
        return self.json

    def download(self, out_dir):
        # Download the file to the specified directory from self.s3 list using pathlib library
        for url in self.s3:
            file_name = url.split('/')[-1]
            out_file = Path(out_dir) / file_name
            if not out_file.exists():
                print(f'Downloading {file_name} to {out_dir}')
                urllib.request.urlretrieve(url, out_file)
            else:
                print(f"{file_name} already exists in {out_dir}")

class Links:
    # class to handle links in the granule json object
    def __init__(self, links):
        self.links = links

    def get_s3(self):
        return [l['href'] for l in self.links if 's3' in l['href'] and '.tif' in l['href']]

    def get_https(self):
        return [l['href'] for l in self.links if 'https' in l['href'] and '.tif' in l['href']]

    def __repr__(self):
        return f'{self.links}'
class CmrSearch:
    def __init__(self, api_key: str):
        self.client = CMRClient(api_key=api_key)

    def search(self, keyword: str, temporal: str) -> List[Dict[str, str]]:
        results = self.client.search(keyword, temporal=temporal)
        return self.parse_results(results)

    def filter(self, results: List[Dict[str, str]], access_level: str) -> List[Dict[str, str]]:
        filtered_results = [
            result for result in results
            if result['access_level'] == access_level
        ]
        return filtered_results

    def parse_results(self, results: List[Dict]) -> List[Dict[str, str]]:
        parsed_results = []
        for result in results:
            parsed_result = {
                'title': result.title,
                'summary': result.summary,
                'link': result.link,
                'data_center': result.data_center,
                'data_source': result.data_source,
                'access_level': result.access_level,
                'spatial': result.spatial,
                'temporal': result.temporal,
                'provider': result.provider
            }
            parsed_results.append(parsed_result)
        return parsed_results


class Earthdata:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.session = Session()

    def login(self):
        # Set up the login request
        login_url = 'https://urs.earthdata.nasa.gov/login'
        login_data = {'username': self.username, 'password': self.password}

        # Send the login request
        response = self.session.post(login_url, data=login_data)

        # Check the response status code
        if response.status_code != 200:
            raise Exception('Failed to log in: {}'.format(response.status_code))

    def search(self, dataset, start_date, end_date):
        # Set up the search request
        search_url = 'https://search.earthdata.nasa.gov/search'
        search_params = {
            'dataset': dataset,
            'temporal': start_date + 'Z' + end_date + 'Z',
            'format': 'json'
        }

        # Send the search request
        response = self.session.get(search_url, params=search_params)

        # Check the response status code
        if response.status_code != 200:
            raise Exception('Failed to search: {}'.format(response.status_code))

        # Parse and return the search results
        return response.json()['feed']['entry']

    def download(self, url, output_dir):
        # Send the download request
        response = self.session.get(url)

        # Check the response status code
        if response.status_code != 200:
            raise Exception('Failed to download: {}'.format(response.status_code))

        # Save the image to the output directory
        filename = url.split('/')[-1]
        with open(os.path.join(output_dir, filename), 'wb') as f:
            f.write(response.content)
