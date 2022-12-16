import os
import sys
import json
import pprint
import netrc
from datetime import datetime
import requests

from requests import Session

# Generate a NASA Earthdata Login Token


class CMRClient:
    """
    A Python API for interacting with the NASA Common Metadata Repository (CMR).

    Attributes:
    - base_url (str): The base URL for the CMR API.
    """

    def __init__(self):
        self.base_url = "https://cmr.earthdata.nasa.gov/search"

    def search_granules(self, params):
        """
        Search the CMR for granules.

        Parameters:
        - params (dict): A dictionary of query parameters to use in the search.
          For a full list of available parameters and their meanings, see the
          CMR API documentation:
          https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#

        Returns:
        - dict: A dictionary of search results, in the same format as the JSON
          response returned by the CMR API.
        """
        url_ = self.base_url + '/granules.json'
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()


    def get_metadata(self, id):
        """
        Get a single collection or granule from the CMR.

        Parameters:
        - id (str): The unique identifier for the collection to retrieve.

        Returns:
        - dict: A dictionary of collection metadata, in the same format as the
          JSON response returned by the CMR API.
        """
        url = f"{self.base_url}/{id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
        
    def parse_granule_metadata(self, granule_json):
        """
        Parse the metadata for a single granule.

        Parameters:
        - granule (dict): A dictionary of granule metadata, as returned by the
          `get_granule()` method.

        Returns:
        - dict: A dictionary of parsed granule metadata, with specific metadata
          fields extracted and formatted for easier access.
        """
        metadata = {}

        # Extract the granule ID
        metadata["id"] = granule["id"]

        # Extract the granule title
        metadata["title"] = granule["title"]

        # Extract the granule summary
        metadata["summary"] = granule["summary"]

        # Extract the granule spatial bounds
        spatial = granule["geo"]["spatial"]
        metadata["spatial"] = {
            "north": spatial["north"],
            "south": spatial["south"],
            "east": spatial["east"],
            "west": spatial["west"],
        }

        # Extract the granule temporal bounds
        temporal = granule["time"]["start"]
        metadata["temporal"] = {
            "start": temporal,
            "end": granule["time"]["end"],
        }

        # Extract the granule data format
        metadata["format"] = granule["data_format"]

        return metadata

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

    def parse_results(self, results: List[SearchResult]) -> List[Dict[str, str]]:
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