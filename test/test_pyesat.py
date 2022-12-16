import os
import sys

import pyesat.earthdata


def test_search_cmr():
    # Import the search_cmr function
    from pyesat.earthdata import search_cmr

    # Search for collections with the keyword "landsat"
    results = search_cmr("keyword:landsat")

    # Make sure the results are not empty
    assert results["feed"]["entry_count"] > 0

    # Make sure each collection has a title and ID
    for entry in results["feed"]["entry"]:
        assert "title" in entry
        assert "id" in entry

def test_earthdata_login(): 
    # Tests the Earthdata.login() method to ensure that it correctly logs the user in to the Earthdata Cloud.
    pyesat.earthdata.
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