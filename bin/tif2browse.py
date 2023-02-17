import os
import sys

import pathlib

_tif_format = 'https://e4ftl01.cr.usgs.gov//ECOB/ECOSTRESS/ECO2LSTE.001/2018.07.28/ECOSTRESS_L2_LSTE_00344_009_20180728T224023_0601_03.h5'
_jpg_format = 'https://e4ftl01.cr.usgs.gov//WORKING/BRWS/Browse.001/2020.10.10/ECOSTRESS_L2_LSTE_00344_010_20180728T224115_0601_03.1.jpg'

def test_swap_format():
    assert swap_format(_tif_format) == _jpg_format


def swap_format(tif_format):
    # input url in tif_format and return string in jpg_format
    # https://e4ftl01.cr.usgs.gov//ECOB/ECOSTRESS/ECO2LSTE.001/2018.07.28/ECOSTRESS_L2_LSTE_00344_009_20180728T224023_0601_03.h5
    # https://e4ftl01.cr.usgs.gov//WORKING/BRWS/Browse.001/2020.10.10/ECOSTRESS_L2_LSTE_00344_009_20180728T224023_0601_03.1.jpg

    # split the url into a list
    tif_url_list = tif_format.split('/')
    jpg_url_list = _jpg_format.split('/')
    url_list_ = jpg_url_list.copy()
    url_list_[7] = tif_url_list[7]
    #'ECOSTRESS_L2_LSTE_00344_010_20180728T224115_0601_03.1.jpg'
    # ECOSTRESS_L2_LSTE_00344_009_20180728T224023_0601_03.h5
    url_list_[8] = tif_url_list[8].replace('.h5', '.1.jpg')
    # combine the list into a string
    jpg_format = '/'.join(url_list_)
    return jpg_format

def main():
    # test the swap_format function
    test_swap_format()
    # open the file
    with open('bin/esat_jpgs.txt', 'w') as f_out, open('bin/esat_tifs.txt', 'r') as f:
        tif_format = f.readline()
        jpg_format = swap_format(tif_format)
        f_out.write(jpg_format)
        print(f'click: {tif_format}-->{jpg_format}')

if __name__ == '__main__':
    
    main()

