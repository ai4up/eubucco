
import os 
import sys
import json
import re
from glob import glob
from io import BytesIO

import pandas as pd
import geopandas as gpd
import requests
import urllib.parse


def get_size(url, params):
    params = params.copy()
    params['resultType'] = 'hits'
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        size = re.findall(r'numberMatched="[0-9]+"', str(response.content))
        size = int(size[0].split('=')[-1][1:-1])
        return size
    return 'unknown'


def process_wfs(region_name,
                size,
                url,
                params,
                count,
                start=0,
                path_data=None,):
    
    i = start
    print(type(size), type(count))
    for i in range(start, size+count, count):        
        print(i)
        params['count'] = count,
        params['startIndex'] = i,
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(i, count)
            print(response.content)
            break

        try:
            gdf = gpd.read_file(BytesIO(response.content))
        except Exception as e:
            print(e)
            break
            
        gdf.to_parquet(os.path.join(path_data, "raw",f"buildings_{region_name}_{i}_raw.pq"))


def process_gdf(region_name,
                url_prefix,
                params,
                count,
                path_data):
    
    for i in range(params['start'], params['stop'], count):
        url = url_prefix+str(i)+params['url_postfix']
        print(url)
        gdf = gpd.read_file(url)
        print(f'{i}: {len(gdf)} Buildings')
        gdf.to_parquet(os.path.join(path_data,'raw', f"buildings_{region_name}_{i}_raw.pq"))


def process_links(region_name,
                url,
                path_data):
    
    links = gpd.read_file(url)
    for i,link in enumerate(links.zip.values):
        link = urllib.parse.quote(link, safe=':/')
        gdf = gpd.read_file(link, layer='gebaeude')
        print(f"{i}: {len(gdf)} Buildings from {link.split('/')[-1][:-9]} (special case info)")
        gdf.to_parquet(os.path.join(path_data,'raw', f"buildings_{region_name}_{i}_raw.pq"))
 

def safe_parquet(region, path_data):

    files = glob(os.path.join(path_data, 'raw',f'buildings_{region}*'))
    frames = []
    for file in files:
        gdf = gpd.read_parquet(file)
        if gdf.shape[0]:
            frames.append(gdf.to_crs(epsg=3035))
    
    gdf = pd.concat(frames, ignore_index=True)

    if 'gml_id' in gdf.columns:
        gdf = gdf[~gdf['gml_id'].duplicated()]
    
    if 'oid' in gdf.columns:
        gdf = gdf[~gdf['oid'].duplicated()]
    
    if 'id' in gdf.columns:
        gdf = gdf[~gdf['id'].duplicated()]

    print("buildings processed: ", len(gdf))
    print("building columns: ", gdf.columns)
    print("building crs: ", gdf.crs)

    gdf.to_parquet(os.path.join(path_data, f'buildings_{region}.pq'))


def init(path_data):
    path_data_raw = os.path.join(path_data, 'raw')
    if not os.path.exists(path_data_raw):
        os.makedirs(path_data_raw)


def get_input():
    request = json.load(sys.stdin)
    print(request)
    return request


def main():
     
    request = get_input()
    init(request['path_data'])

    if 'size' in request['jobs']:
        size = get_size(request['url'], request['params'])
        print(f"Number of files: {size}")

    if ('size' in request['jobs']) & ('wfs' in request['jobs']):
        process_wfs(request['region'],
                    size,
                    request['url'],
                    request['params'],
                    request['count'],
                    start=0, # left at default
                    path_data = request['path_data'])

    if "gdf" in request['jobs']:
        process_gdf(request['region'],
                    request['url'],
                    request['params'],
                    request['count'],
                    request['path_data'])
    
    if "links" in request['jobs']:
        process_links(request['region'],
                    request['url'],
                    request['path_data'])

    if 'parquet' in request['jobs']:
        safe_parquet(request['region'],
                        request['path_data'])


if __name__ == "__main__":
    main() 

