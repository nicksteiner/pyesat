#!/bin/python

import os
import sys
import json
from netrc import netrc

import configparser
import pathlib
import datetime
from getpass import getpass

import requests

sys.path.append('..')  # NOTE: for debugging, should change when installed

import pyesat.credentials as credentials

config_file = pathlib.Path(credentials.__file__).parent / 'config.ini'


def main():
    if not credentials.check_netrc():
        assert credentials.write_netrc()
    assert credentials.write_earthdata_credentials()
    #assert credentials.write_s3_credentials()
    print('Done!')

if __name__ == '__main__':
    main()
