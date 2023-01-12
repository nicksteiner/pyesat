import os
import sys
import json
import concurrent.futures
import dask
import dask.array as da
from dask.diagnostics import ProgressBar

from tqdm import tqdm
import boto3
from typing import List, Dict
from pathlib import Path
import urllib
import rioxarray
from rasterio.session import AWSSession
from datetime import datetime
import requests
import xarray as xr
import rasterio as rio
from requests import Session

from . import credentials
# Generate a NASA Earthdata Login Token


def set_rio_environment(daac: str='lpdaac') -> bool:
    temp_creds_req = credentials.get_daac_credentials(daac)
    session = boto3.Session(aws_access_key_id=temp_creds_req['access_key'],
                            aws_secret_access_key=temp_creds_req['secret_key'],
                            aws_session_token=temp_creds_req['session_token'],
                            region_name='us-west-2')
    cookie_path = Path.home() / 'cookies.txt'
    rio_env = rio.Env(AWSSession(session),
                      GDAL_DISABLE_READDIR_ON_OPEN='TRUE',
                      GDAL_HTTP_COOKIEFILE=cookie_path.as_posix(),
                      GDAL_HTTP_COOKIEJAR=cookie_path.as_posix())
    rio_env.__enter__()
    #return rio_env
    return True


class DaacReadSession:
    def __init__(self, daac: str='lpdaac'):
        self.daac = daac
        self.temp_creds_req = credentials.get_daac_credentials(self.daac)
        self.session = None
        exp_date = self.temp_creds_req['expiration_date']
        self.expiration_date = datetime.strptime(exp_date, "%Y-%m-%d %H:%M:%S%z")

    def __enter__(self):
        if not self.is_expired():
            self.session = self._get_session()
        else:
            self.temp_creds_req = credentials.get_daac_credentials(self.daac)
            self.session = self._get_session()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)

    def is_expired(self) -> bool:
        return self.expiration_date < datetime.now(credentials._tz)

    def _get_session(self) -> Session:
        session = boto3.Session(aws_access_key_id=self.temp_creds_req['access_key'],
                                aws_secret_access_key=self.temp_creds_req['secret_key'],
                                aws_session_token=self.temp_creds_req['session_token'],
                                region_name='us-west-2')

        cookie_path = Path.home() / '.aws' / 'cookies'
        rio_env = rio.Env(AWSSession(session),
                          GDAL_DISABLE_READDIR_ON_OPEN='TRUE',
                          GDAL_HTTP_COOKIEFILE=cookie_path.as_posix(),
                          GDAL_HTTP_COOKIEJAR=cookie_path.as_posix())
        rio_env.__enter__()
        return rio_env



class CMRClient:
    """
    A Python API for interacting with the NASA Common Metadata Repository (CMR).

    Attributes:
    - base_url (str): The base URL for the CMR API.
    """

    def __init__(self, provider='LPCLOUD', project='ECOSTRESS'):
        self.base_url = 'https://cmr.earthdata.nasa.gov'
        self.search_url = f"{self.base_url}/search"
        self.access_token = credentials.read_earthdata_token()
        self.provider = provider
        self.project = project
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

    def get_collections(self, verbose=True):
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

    def __init__(self, granule, keep_xarray=False, verbose=True):
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
        self.keep_xarray = keep_xarray
        self.xarray = None
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

    def get_xarray(self, data_sets=None, aws=True, verbose=True) -> xr.Dataset:
        # Return an xarray dataset from the file in self.https list
        # https://xarray.pydata.org/en/stable/generated/xarray.open_dataset.html
        # https://xarray.pydata.org/en/stable/io.html#reading-from-amazon-s3

        with DaacReadSession() as session:
            if aws:
                links = self.s3
            else:
                links = self.https
            if data_sets is None:
                data_sets = [f.split('_')[-1].replace('.tif', '') for f in links]
            if verbose:
                data_sets_ = [f.split('_')[-1].replace('.tif', '') for f in links]
                data_sets_ = {ds: url for ds, url in zip(data_sets_, links)}
                data_urls = [data_sets_[ds] for ds in data_sets]
            loc_ = {True: 'S3', False: 'HTTPS'}[aws]
            if verbose:
                print(f'Opening {loc_} {self.id} with data sets: {data_sets}')
            data_array = {ds:dask.delayed(rioxarray.open_rasterio)(ds_url, chunks='auto') for ds, ds_url in
                          zip(data_sets, data_urls)}
            with ProgressBar():
                data_set = xr.Dataset({ds:d.compute().squeeze() for ds, d in data_array.items()})
        # add time coordinate to data set and set as dimension coordinate

        print(f'Finished opening {self.id}')
        data_set['time'] = self.time_start
        # add the time coordinate as a dimension coordinate
        data_set = data_set.set_coords('time')
        data_set = data_set.expand_dims(dim='time', axis=0)
        # drop the band and spatial_ref coordinate variables
        data_set = data_set.drop_vars(['spatial_ref', 'band'])
        # add the granule metadata as attributes to the data set
        data_set.attrs['id'] = self.id
        data_set.attrs['dataset_id'] = self.dataset_id
        data_set.attrs['data_center'] = self.data_center
        data_set.attrs['time_start'] = self.time_start
        data_set.attrs['time_end'] = self.time_end
        data_set.attrs['collection_concept_id'] = self.collection_concept_id
        data_set.attrs['producer_granule_id'] = self.producer_granule_id
        data_set.attrs['browse_flag'] = self.browse_flag
        data_set.attrs['online_access_flag'] = self.online_access_flag
        data_set.attrs['original_format'] = self.original_format
        data_set.attrs['coordinate_system'] = self.coordinate_system
        data_set.attrs['day_night_flag'] = self.day_night_flag
        data_set.attrs['title'] = self.title
        data_set.attrs['updated'] = self.updated
        data_set.attrs['granule_size'] = self.granule_size
        data_set.attrs['start_orbit_number'] = self.start_orbit_number
        data_set.attrs['stop_orbit_number'] = self.stop_orbit_number
        data_set.attrs['boxes'] = self.boxes
        data_set.attrs['links'] = self.links
        data_set.attrs['s3'] = self.s3
        data_set.attrs['https'] = self.https
        # the most pythonic way to retain the xarray dataset in the object if you want to use it later
        if self.keep_xarray:
            self.xarray = data_set
        return data_set

    def write_to_zarr(self, path):
        # check if there is an xarray dataset in the object if not
        # load it make sure to append to other data that may be in the zarr store
        if self.xarray is None:
            self.get_xarray()
        # write the xarray dataset to a zarr file in the specified path
        self.xarray.to_zarr(path, mode='a')
        print(f'Finished writing {self.id} to {path}')

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


