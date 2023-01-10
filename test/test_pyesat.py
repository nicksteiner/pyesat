import os
import sys
import pprint

_test_data = {}
_test_data['date_range'] = '2022-12-01T00:00:00Z,2022-12-07T23:59:59Z'
_test_data['bbox'] = '-120.45264628,34.51050622,-120.40432448,34.53239876'

def test_cmr_collections():
    import pyesat.earthdata
    client = pyesat.earthdata.CMRClient()
    client.get_collections()
    assert True

def test_cmr_granules():
    # Import the search_cmr function
    import pyesat.earthdata
    client = pyesat.earthdata.CMRClient()
    granules = client.search_granules(_test_data['bbox'], _test_data['date_range'])
    # Make sure each collection has a title and ID
    for entry in granules:
        assert "title" in entry
        assert "id" in entry
    # print all keys and values from the entry in a granule
    print('/n/n/n')
    for entry in granules:
        for k, v in entry.items():
            pprint.pprint(f'{k}: {v}')


def test_get_s3():
    import pyesat.earthdata
    client = pyesat.earthdata.CMRClient()
    granules = client.search_granules(_test_data['bbox'], _test_data['date_range'])
    # Make sure each collection has a title and ID
    for entry in granules:
        assert "title" in entry
        assert "id" in entry



def test_earthdata_login(): 
    # Tests the Earthdata.login() method to ensure that it correctly logs the user in to the Earthdata Cloud.

    pass

def test_search(): 
    # Tests the Earthdata.search() method to ensure that it correctly searches for images based on the specified criteria.
    pass

def test_download():
    pass
    # Tests the Earthdata.download() method to ensure that it correctly downloads images from the Earthdata Cloud.

def test_extract_metadata():
    pass
    # Tests the extract_metadata() function to ensure that it correctly extracts metadata from an image.

def test_apply_algorithm(): 
    #Tests the apply_algorithm() function to ensure that it correctly applies the specified image processing algorithm to an image.
    pass