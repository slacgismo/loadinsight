"""
This module downloads Commercial End-Use Survey (CEUS) files into intended filepath.
"""

import requests

def CEUS_download(FCZs, buildingtypes, ceus_url, filepath):
    """CEUS downloader for FCZ, buildingtype combo from the download url
    files are downloaded onto filepath
    """
    for FCZ in FCZs:
        for buildingtype in buildingtypes:

            filename = FCZ+'_'+buildingtype+'.xls'
            download_url = ceus_url+filename

            if is_downloadable(download_url):
                file = requests.get(download_url)
                open(filename, 'wb').write(file.content)
                
    return

def is_downloadable(url):
    """
    Does the url contain a downloadable resource
    """
    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get('content-type')

    if 'text' in content_type.lower():
        raise Exception("CEUS file is not downloadable from following link:", url)
        return False

    if 'html' in content_type.lower():
        raise Exception("CEUS file is not downloadable from following link:", url)
        return False

    return True