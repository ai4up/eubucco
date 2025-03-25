
from glob import glob
import geopandas as gpd
from io import BytesIO
import re
import requests
import pandas as pd
import json, sys


# ger_regions = ['bavaria', 'brandeburg', 'bw', 'hessen', 'mv',
#        'nrw', 'rlp', 'saarland', 'sachsen', 'sachsen-anhalt', 'sg', 
#        'th', 'ni', 'bremen', 'hamburg', 'berlin']


def process_wfs(region_name,
                size,
                url,
                params,
                count,
                start=0,
                path_data=None,
                ):
    params = params.copy()
    i = start
    for i in range(start, size+count, count):
        
        print(i)
        params['count'] = count,
        params['startIndex'] = i,

        # Make the request
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
            
        gdf.to_parquet(path_data + f"buildings_{region_name}_{i}_raw.pq")


def get_size(url, params):
    params = params.copy()
    params['resultType'] = 'hits'
    response = requests.get(url, params=params)
    if response.status_code == 200:
        size = re.findall(r'numberMatched="[0-9]+"', str(response.content))
        size = int(size[0].split('=')[-1][1:-1])
        return size
    return 'unknown'


def process_parquet(region, path_data=None):
    files = glob(f'{path_data}buildings_germany_{region}*')
    frames = []
    for f in files:
        gdf = gpd.read_parquet(f)
        if gdf.shape[0]:
            frames.append(gdf.to_crs(epsg=3035))
    
    gdf = pd.concat(frames, ignore_index=True)

    if 'gml_id' in gdf.columns:
        gdf = gdf[~gdf['gml_id'].duplicated()]
    if 'oid' in gdf.columns:
        gdf = gdf[~gdf['oid'].duplicated()]

    print(gdf.shape)
    
    gdf.to_parquet(path_data, f'buildings_germany_{region}.pq')


def get_input():
    #  check if on cluster or locally mounted
    request = json.load(sys.stdin)
    print(request)
    return request


def main():
     
    request = get_input()
    
    jobs = request['jobs']
    region = request['region']
    params = request['params']
    count = request['count']
    path_data = request['path_data']
    path_data_processed = request['path_data_processed']
    url = request['url']


    # Parameters for the GetFeature request
    if 'size' in jobs:
        size = get_size(url, params)
        print(size)
    
    if ('size' in jobs) & ('wfs' in jobs):
        process_wfs("germany_brandeburg",
                    size,
                    url,
                    params,
                    count,
                    path_data=path_data)
        
    if 'parquet' in jobs:
        process_parquet(region,
                        path_data = path_data,
                        path_data_processed = path_data_processed)

if __name__ == "__main__":
    main() 

