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

import pyesat

config_file = pathlib.Path(pyesat.__file__).parent / 'config.ini'

config_info_str = """Please write a file config.ini in the /pyesat directory with the format:

        [urs.earthdata.nasa.gov]
          username = <mylogin>
          password = <mypassword>
        """

remoteHostName = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication

_edl_token_urls = {
    'generate_token': 'https://urs.earthdata.nasa.gov/api/users/token',
    'list_token': 'https://urs.earthdata.nasa.gov/api/users/tokens',
    'revoke_token': 'https://urs.earthdata.nasa.gov/api/users/revoke_token'
}


def write_config(config_state):
    with open(config_file.as_posix(), 'w') as f:
        config_state.write(f)
    os.chmod(config_file.as_posix(), 0o640)


def write_config_fromPrompt():
    # write documentation for this function
    config_state = configparser.ConfigParser()

    username = input("Enter NASA Earthdata username Username ")
    password = getpass("Enter NASA Earthdata username Password: ")
    config_state[remoteHostName] = {}
    config_state[remoteHostName]['username'] = username
    config_state[remoteHostName]['password'] = password

    write_config(config_state)
    # check if entry is in netrc file and write if not
    try:
        netrc_file = netrc()
        netrc_file.authenticators(remoteHostName)
    except:
        # write entry into netrc file
        netrc_file = netrc()
        netrc_file.addauth(remoteHostName, username, password, None)




def get_config(config_parser):
    return config_parser.read_file(open(config_file, 'r'))


def main():
    config_parser = configparser.ConfigParser()
    # Determine if netrc file exists, and if so, if it includes NASA Earthdata username Credentials
    try:
        get_config(config_parser)
    except:
        print('File not found will create from prompt ....')
        write_config_fromPrompt()
        get_config(config_parser)

    try:
        assert remoteHostName in config_parser
        assert 'username' in config_parser['urs.earthdata.nasa.gov']
        assert 'password' in config_parser['urs.earthdata.nasa.gov']
    except:
        write_config_fromPrompt()
        get_config(config_parser)

    auth = (config_parser[remoteHostName]['username'], config_parser[remoteHostName]['password'])
    req_ = requests.get(_edl_token_urls['list_token'], auth=auth)
    if req_.status_code == 401:
        error_ = json.loads(req_.text)
        raise Exception(f"{error_['error']}:{error_['error_description']}")

    tokens_ = req_.json()

    if not tokens_:
        generate_token_req = requests.post(_edl_token_urls['generate_token'], **auth)
        token = generate_token_req.json()
    else:
        # check expired
        dates_ = [datetime.datetime.strptime(t['expiration_date'], "%m/%d/%Y") for t in tokens_]

        token = None
        for (d_, t_) in sorted(zip(dates_, tokens_)):
            if d_ > datetime.datetime.now():
                token = t_
            else:
                revoke_token = requests.post(f"{_edl_token_urls['revoke_token']}?", data={'token': t_}, auth=auth)

        if not token:
            generate_token_req = requests.post(_edl_token_urls['generate_token'], auth=auth)
            token = generate_token_req.json()

    # write token to config
    config_parser[remoteHostName]['token'] = token['access_token']
    write_config(config_parser)
    print(f"Config file written to {os.path.abspath(config_file.as_posix())}")


if __name__ == '__main__':
    main()
