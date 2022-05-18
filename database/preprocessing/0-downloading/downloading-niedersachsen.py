import geopandas as gpd
import requests
import os
import urllib


def download_url(url, save_path, chunk_size=128):
    """
    Function to download urls
    """
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


# specify output foldder
path_folder = '/Volumes/ssd_500gb/Drive/Downloads/niedersachsen2'

# read in geojson file; in column 3DShape we have now all download links!
df = gpd.read_file(
    '/Users/Felix/Documents/Studium/PhD/05_Projects/02_Estimate_Building_Heights/preprocessing/Webscraper/lgln-opengeodata-lod2.geojson')

# loop over all links to downlaod the zip files
for i in range(len(df)):
    # takes path + url string and takes second part of split at last '/' to define output file name
    path = os.path.join(path_folder, df['3DShape'][i].rsplit('/', 1)[1])
    # download
    download_url(df['3DShape'][i], path)

    if i % 100 == 0:
        print('-------------')
        print('Number of Downloaded files: ', i)
        print('-------------')
