import os
import sys


def test_cmr_collections():
    import pyesat.earthdata
    client = pyesat.earthdata.CMRClient()
    client.get_collections()
    assert True

def test_cmr_granules():
    # Import the search_cmr function
    import pyesat.earthdata
    client = pyesat.earthdata.CMRClient()
    date_range = '2022-12-01T00:00:00Z,2022-12-15T23:59:59Z'
    bbox = '-120.45264628,34.51050622,-120.40432448,34.53239876'
    granules = client.search_granules(bbox, date_range)

    ## Search for collections with the keyword "landsat"
    #results = search_cmr("keyword:landsat")

    # Make sure the results are not empty
    #assert results["feed"]["entry_count"] > 0

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