import os
import sys
import json
import pytz
from getpass import getpass
from typing import Dict, Tuple
from pathlib import Path
import netrc
import configparser
import datetime

import requests

# all credential is stored in this file, keep secure, will be used by other scripts
config_file = Path(__file__).parent / 'config.ini'  # same directory as the source
global tz
tz = pytz.timezone("America/New_York")

_remote_hostname = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication

# earthdata credentials endpoints
_edl_token_urls = {
    'generate_token': 'https://urs.earthdata.nasa.gov/api/users/token',
    'list_token': 'https://urs.earthdata.nasa.gov/api/users/tokens',
    'revoke_token': 'https://urs.earthdata.nasa.gov/api/users/revoke_token'
}

# s3 access credentials
_s3_cred_endpoint = {
    'podaac': 'https://archive.podaac.earthdata.nasa.gov/s3credentials',
    'gesdisc': 'https://data.gesdisc.earthdata.nasa.gov/s3credentials',
    'lpdaac': 'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials',
    'ornldaac': 'https://data.ornldaac.earthdata.nasa.gov/s3credentials',
    'ghrcdaac': 'https://data.ghrc.earthdata.nasa.gov/s3credentials'
}

config_info_str = """Please write a file config.ini in the /pyesat directory with the format:

        [urs.earthdata.nasa.gov]
          username = <mylogin>
          password = <mypassword>
        """


def get_earthdata_login() -> Tuple[str, str]:
    # get the username and password from the configuration file
    try:
        netrc_file = netrc.netrc()
        # check if the remote_hostname is in the netrc file
        assert _remote_hostname in netrc_file.hosts
    # write a more clear exception clause
    except netrc.NetrcParseError:
        print('Netrc file not found will create from prompt ....')
        assert write_netrc()
        netrc_file = netrc.netrc()
    auth = (netrc_file.authenticators(_remote_hostname)[0],
            netrc_file.authenticators(_remote_hostname)[2])
    return auth


def check_netrc():
    # check if the netrc file exists
    path_to_netrc = Path.home() / ".netrc"
    if path_to_netrc.exists():
        netrc_file = netrc.netrc()
        # check if the remote_hostname is in the netrc file
        if _remote_hostname in netrc_file.hosts:
            return True
    return False


def write_netrc() -> bool:
    # write netrc file from prompt user/password
    path_to_netrc = Path.home() / ".netrc"
    if not path_to_netrc.exists():
        path_to_netrc.touch()
        path_to_netrc.chmod(0o600)
    # write netrc file from prompt user/password
    login = input("Enter NASA Earthdata username Username ")
    password = getpass("Enter NASA Earthdata username Password: ")
    netrc_file = netrc.netrc()
    # add auth to netrc file
    netrc_file.hosts[_remote_hostname] = (login, None, password)
    with open(path_to_netrc.as_posix(), 'w') as f:
        f.write(str(netrc_file))
    print('Configuration file written to: ', config_file.as_posix())
    print('Please check the file and make sure it is secure')
    return True


def get_temp_credentials(provider) -> Dict:
    """Generate temporary NASA Earthdata credentials for a given provider
       Requires netrc file to be configured with NASA Earthdata username and password
    """
    return requests.get(_s3_cred_endpoint[provider]).json()


def write_config(config_state) -> bool:
    # write the config file to disk and set permissions
    with open(config_file.as_posix(), 'w') as f:
        config_state.write(f)
    os.chmod(config_file.as_posix(), 0o640)
    return True


def get_earthdata_token() -> Dict:
    # get token for earthdata access
    auth = get_earthdata_login()
    req_ = requests.get(_edl_token_urls['list_token'], auth=auth)
    if req_.status_code == 401:
        error_ = json.loads(req_.text)
        raise Exception(f"{error_['error']}:{error_['error_description']}")
    tokens_ = req_.json()
    if tokens_:
        dates_ = [datetime.datetime.strptime(t['expiration_date'], "%m/%d/%Y") for t in tokens_]
        for (d_, t_) in sorted(zip(dates_, tokens_)):
            if d_ > datetime.datetime.now():
                token = t_
            else:
                revoke_token = requests.post(f"{_edl_token_urls['revoke_token']}?", data={'token': t_}, auth=auth)
                if revoke_token.status_code == 401:
                    error_ = json.loads(revoke_token.text)
                    raise Exception(f"{error_['error']}:{error_['error_description']}")
    else:
        generate_token_req = requests.post(_edl_token_urls['generate_token'], auth=auth)
        token = generate_token_req.json()
    # check expired
    return dict(token)


def update_earthdata_token(config_state):
    # update the token
    token = get_earthdata_token()
    config_state['urs.earthdata.nasa.gov']['access_token'] = token['access_token']
    config_state['urs.earthdata.nasa.gov']['expiration_date'] = token['expiration_date']
    write_config(config_state)
    print(f"Config file written to {os.path.abspath(config_file.as_posix())}")
    return True


def write_earthdata_credentials() -> bool:
    # write configuration file from prompt user/password
    # get username and password from netrc file
    config_parser = get_credentials()
    if not config_parser.has_section(_remote_hostname):
        config_parser.add_section(_remote_hostname)
        assert update_earthdata_token(config_parser)
    else:
        try:
            assert check_earthdata_credentials()
        except:
            print('Credentials are not valid, will update from prompt ....')
            assert update_earthdata_token(config_parser)
    return True


def get_credentials() -> configparser.ConfigParser:
    """
    Get the credentials from the configuration file
    Returns: configparser.ConfigParser

    """
    # get the configuration file if exists
    config_parser = configparser.ConfigParser()
    if not config_file.exists():
        config_file.touch()
        config_file.chmod(0o640)
    else:
        try:
            config_parser.read(config_file.as_posix())
        except configparser.MissingSectionHeaderError:
            print('Configuration file is not in the correct format')
            print(config_info_str)
            sys.exit(1)
    return config_parser


def update_daac_credentials(daac, config_parser) -> bool:
    """

    Args:
        daac:   daac name (e.g. lpdaac)
        config_parser: current configuration file state

    Returns: True if successful

    """
    temp_credentials = get_temp_credentials(daac)
    config_parser[daac]['access_key'] = temp_credentials['accessKeyId']
    config_parser[daac]['secret_key'] = temp_credentials['secretAccessKey']
    config_parser[daac]['session_token'] = temp_credentials['sessionToken']
    config_parser[daac]['expiration_date'] = temp_credentials['expiration']
    write_config(config_parser)
    print(f"{daac} credentials written to {config_file.as_posix()}")
    return True


def get_daac_credentials(daac) -> Dict:
    # get daac credentials from config file for reading data
    config_parser = get_credentials()
    if not config_parser.has_section(daac):
        config_parser.add_section(daac)
        update_daac_credentials(daac, config_parser)
    else:
        # check expiration date
        expiration_date_str = config_parser.get(daac, 'expiration_date')
        # write datetime parser for '2023-01-11 19:13:22+00:00' format
        expiration_date = datetime.datetime.strptime(expiration_date_str, "%Y-%m-%d %H:%M:%S%z")
        if expiration_date < datetime.datetime.now(tz):
            update_daac_credentials(daac, config_parser)
    return dict(config_parser[daac])


def check_earthdata_credentials() -> bool:
    # check if the credentials are valid
    check_ = True
    # check if the configuration file exists and is secure
    config_parser = get_credentials()
    try:
        assert config_parser.has_option(_remote_hostname, 'access_token')
        assert config_parser.has_option(_remote_hostname, 'expiration_date')
    except Exception('Missing tokens') as e:
        print(e)
        check_ = False
    # check if the token is still valid
    expiration_date_str = config_parser.get(_remote_hostname, 'expiration_date')
    expiration_date = datetime.datetime.strptime(expiration_date_str, "%m/%d/%Y")
    try:
        assert expiration_date > datetime.datetime.now(tz)
    except:
        check_ = False
    return check_


def read_earthdata_token() -> str:
    """
    This function attempts to read the access token from the earthdata credentials file.
    If the credentials are not found, the function writes the credentials and updates them.

    Returns:
        str: Access token for the earthdata service
    """
    try:
        assert check_earthdata_credentials()
    except AssertionError:
        print('Missing Credentials .. will need to update')
        write_earthdata_credentials()
    return str(get_credentials()[_remote_hostname]['access_token'])


def write_s3_credentials() -> bool:
    for daac in _s3_cred_endpoint:
        get_daac_credentials(daac)
    return True
