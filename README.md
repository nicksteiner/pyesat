<div align="left">
  <img src="img/logo.jpeg" width="80"><br>
</div>



# pyesat: Python library for ECOSTRESS Collection 2


## What is it?

**pyesat** is a Python library that provides an interface to ECOSTRESS data Collection 2, which is a dataset of Earth images collected by NASA's ECOSTRESS instrument on board the International Space Station. The library allows users to easily access and download the data from the Earthdata Cloud, where it is hosted. With pyesat, users can search for specific images and download t hem directly into their Python environment for further analysis and visualization. The library also provides convenient functions for working with the data, such as applying various image processing algorithms and extracting metadata. Overall, pyesat makes it easy for Python users to access and work with ECOSTRESS data Collection 2, enabling them to gain valuable insights into the Earth's surface temperature and plant health.

## Main Features
Here are some of the features that will be included in **pyesat** (under construction):
- Functions for searching and downloading specific images from the dataset
- Tools for working with the data, such as image processing algorithms and metadata extraction
- Convenience functions for analyzing and visualizing the data in Python.
## Where to get it (in development)
The source code is currently hosted on GitHub at:
https://github.com/nicksteiner/pyesat

Binary installers for the latest released version are available at the [Python
Package Index (PyPI)](https://pypi.org/project/pandas) and on [Conda](https://docs.conda.io/en/latest/).

```sh
# conda
conda install (working on it)
```

```sh
# or PyPI
pip install (working on it)
```
## Dependencies
## License
[MIT](LICENSE)

## Usage (in development)
```python
import pyesat
# Search for a scene
results = pyesat.search(lat=37.78, lon=-122.41, dataset='ECOSTRESS', start_date='2020-01-01', end_date='2020-01-31')
```

This will return a list of images that match the specified search criteria. You can then use the `download()` function to download the images you want:  
```python
# Download the first image in the results
image = results[0]
image_data = pyesat.download(image["id"])
```

You can extract metadata from the image:
```python
metadata = pyesat.extract_metadata(results[0])
```  
For more information on how to use pyesat, see the [documentation]().