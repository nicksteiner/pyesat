from setuptools import setup

setup(
    name='pyesat',
    version='0.1.0',
    author='Nick Steiner',
    author_email='nick@nsteiner.com',
    url='https://github.com/nicksteiner/pyesat',
    description='A Python library for accessing satellite data',
    packages=['pyesat'],
    scripts=['bin/write_earthdata_credentials.py'],
    install_requires=[
        'requests>=2.24.0',
        'lxml>=4.6.1'
    ]
)