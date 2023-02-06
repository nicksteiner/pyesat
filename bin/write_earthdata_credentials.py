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
import pyesat.pyesat

config_file = pathlib.Path(pyesat.pyesat.__file__).parent / 'config.ini' 

config_info_str = """Please write a file config.ini in the /pyesat directory with the format:

        [urs.earthdata.nasa.gov]
          username = <mylogin>
          password = <mypassword>
        """

remoteHostName = "urs.earthdata.nasa.gov" # Earthdata URL to call for authentication

_edl_token_urls = {
    'generate_token':'https://urs.earthdata.nasa.gov/api/users/token',
    'list_token':    'https://urs.earthdata.nasa.gov/api/users/tokens',
    'revoke_token':  'https://urs.earthdata.nasa.gov/api/users/revoke_token'
}

def write_config(config_state):
        with open(config_file.as_posix(), 'w') as f:            
            config_state.write(f)
        os.chmod(config_file.as_posix(), 0o640)

def write_config_fromPrompt():
        config_state = configparser.ConfigParser()
        username = input("Enter NASA Earthdata username Username ")
        password = getpass.getpass("Enter NASA Earthdata username Password: ")
        config_state[remoteHostName] = {}
        config_state[remoteHostName]['username'] = username
        config_state[remoteHostName]['password'] = password
        write_config(config_state)


def main():
    if not credentials.check_netrc():
        assert credentials.write_netrc()
    assert credentials.write_earthdata_credentials()
    #assert credentials.write_s3_credentials()
    print('Done!')

if __name__ == '__main__':
    main()
