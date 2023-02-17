import os
import sys
import pathlib
import getpass
import netrc
import pycurl
import requests

import http.cookiejar
import urllib.request



test_url = "https://e4ftl01.cr.usgs.gov//ECOB/ECOSTRESS/ECO2LSTE.001/2018.07.28/ECOSTRESS_L2_LSTE_00344_009_20180728T224023_0601_03.h5"

def test_cookies():
    # Create a cookie jar
    cookie_jar = http.cookiejar.LWPCookieJar()

    # Load cookies from a file
    cookie_jar.load("cookies.txt", ignore_discard=True, ignore_expires=True)

    # Create an opener using the cookie jar
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))

    # Use the opener to perform a request
    response = opener.open(test_url)

    # Read the response
    html = response.read().decode()

    # Print the response
    print(html)

def check_credentials():
    cookiejar_file = "$cookiejar"

    # Check if the cookies file exists
    if os.path.isfile(cookiejar_file):
        # Create a Curl object
        curl = pycurl.Curl()
        
        # Set the URL
        curl.setopt(pycurl.URL, test_url)

        # Set the maximum number of redirects
        curl.setopt(pycurl.MAXREDIRS, 5)

        # Set the cookiejar file
        curl.setopt(pycurl.COOKIEJAR, cookiejar_file)
        curl.setopt(pycurl.COOKIEFILE, cookiejar_file)

        # Set the write function
        curl.setopt(pycurl.WRITEFUNCTION, lambda x: None)

        # Perform the request
        curl.perform()

        # Get the HTTP response code
        http_code = curl.getinfo(pycurl.HTTP_CODE)

        # Close the Curl object
        curl.close()

        # Print the HTTP response code
        print("HTTP response code:", http_code)
    else:
        print("The cookies file '{}' does not exist.".format(cookiejar_file))

def detect_app_approval():
    
    approved = os.popen(f"curl -s -b \"$cookiejar\" -c \"$cookiejar\" -L --max-redirs 5 --netrc-file \"$netrc\" https://e4ftl01.cr.usgs.gov//ECOB/ECOSTRESS/ECO2LSTE.001/2018.07.28/ECOSTRESS_L2_LSTE_00344_009_20180728T224023_0601_03.h5 -w %{http_code} | tail  -1").read()
    try:
        assert approved in ["200", "302"]
    except AssertionError:
        raise AssertionError(f"Error: {approved} != 200 or 302\n" + "Please ensure that you have authorized the remote application by visiting the link below ")
    print("Success: You have authorized the remote application")


def main():
    test_cookies()
    check_credentials()

    detect_app_approval()

if __name__ == '__main__':
    main()
