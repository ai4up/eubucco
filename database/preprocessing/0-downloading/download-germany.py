
import os 
import sys
import json
import re
from glob import glob
from io import BytesIO
import zipfile
import pathlib
import xml.etree.ElementTree as ET

import pandas as pd
import geopandas as gpd
from shapely import wkb
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
        gdf = gpd.read_file(url)
        print(f"{i}: {len(gdf)} Buildings from {url.rsplit('/',1)[1]}")
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


def _list(x):
    if isinstance(x, str): return [x]
    else: return x


def process_zip(region_name,
                urls,
                path_data):
    
    print("creating zip dir")
    path_zip_dir = os.path.join(path_data, 'raw',f'zip_{region_name}')
    pathlib.Path(path_zip_dir).mkdir(exist_ok=True)
    
    urls = _list(urls)
    print("downloading zip file")
    for i, url in enumerate(urls):
        filename = url.rsplit("/",1)[1].rsplit(".",1)[0]
        
        print(f'Downloading {i}/{len(urls)}: {filename}')
        response = requests.get(url)
     
        print("extracting zip")
        z = zipfile.ZipFile(BytesIO(response.content))
        for zipinfo in _list(z.infolist()):
            # add filename as pre-fix to avoid overwriting from dirs with same file names
            zipinfo.filename = filename+'_'+zipinfo.filename
            z.extract(zipinfo, path=path_zip_dir)


def process_xml(region_name,
                url,
                path_data,
                params):
    
    print("downloading xml file")
    response = requests.get(url)

    # Load the XML file
    tree = ET.parse(BytesIO(response.content))
    root = tree.getroot()

    # Find all elements with the tag 'entry'
    entries = root.findall('atom:entry', params['namespaces'])
    codes = []

    # Extract and print the 'inspire_dls:spatial_dataset_identifier_code' attribute for each entry
    for entry in entries:
        # Find the 'inspire_dls:spatial_dataset_identifier_code' element within the entry
        spatial_code = entry.find(params['spatial_identifier_code'], params['namespaces'])
        codes.append(spatial_code.text)    
    
    for code in codes:
        url_bld_code = params['bld_url_pre_fix']+code+params['bld_url_post_fix']
        gdf = gpd.read_file(url_bld_code, driver='GML')
        gdf.to_parquet(os.path.join(path_data, 'raw', f'buildings_{region_name}_{code}_raw.pq'))


def _read_geofile(file, region):
    if pathlib.Path(file).suffix == ".pq":
        return gpd.read_parquet(file)
    elif region == "hamburg":
        return gpd.read_file(file, layer='building').set_crs(epsg=25832)
    else:
        return gpd.read_file(file)


def _handle_missing_metadata(file,data_errors, geom_errors):
    """ 
    handles errors of missing medata data (data error) or
    missing geometry column (geom error) 
    """
    print(f"Missing geo metadata error in {file} - using pandas fallback")
    data_errors.append(file)
    df = pd.read_parquet(file)
    
    try: 
        df["geometry"] = df["geometry"].apply(wkb.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf.set_crs("EPSG:3035", inplace=True)
    
    except KeyError:
        print(f"Missing geometry column in {file} - skipping")
        geom_errors.append(file)
        gdf = gpd.GeoDataFrame() # empty gdf will not be appended to frame

    return data_errors, geom_errors


def _remove_id_duplicates(gdf):
    if 'gml_id' in gdf.columns:
        gdf = gdf[~gdf['gml_id'].duplicated()]
    
    if 'oid' in gdf.columns:
        gdf = gdf[~gdf['oid'].duplicated()]
    
    if 'id' in gdf.columns:
        gdf = gdf[~gdf['id'].duplicated()]

    return gdf


def safe_parquet(region, path_data, params):
    if "file_type" in params.keys():
        # for zip cases - file types can be different per state and must be specified in params
        path_files = os.path.join(path_data, 'raw',f"zip_{region}/**/*.{params['file_type']}")
    else:
        path_files = os.path.join(path_data, 'raw',f'buildings_{region}*')
    
    files = glob(path_files,recursive=True)
    frames = []
    data_errors = []
    geom_errors = []
    
    for file in files:
        try: 
            gdf = _read_geofile(file, region)
        
        except ValueError:
           data_errors, geom_errors = _handle_missing_metadata(file,
                                                                data_errors,
                                                                geom_errors)

        if gdf.shape[0]:
            print(f"appending geoms of file:{file}")
            frames.append(gdf.to_crs(epsg=3035))
    
    gdf = pd.concat(frames, ignore_index=True)
    gdf = _remove_id_duplicates(gdf)

    print("-"*12)
    print("Run summary:")
    print("buildings processed: ", len(gdf))
    print("building columns: ", gdf.columns)
    print("building crs: ", gdf.crs)
    print(f"Found {len(data_errors)} metadata errors (ValueError)")
    print(f"Found {len(geom_errors)} missing geom errors (KeyError)")
    
    if len(data_errors) > 0:    
        print("*"*8)
        print("Metadata errors in following files:")
        print("\n".join(data_errors))
        print("*"*8)
        print("Missing geom errors in following files:")
        print("\n".join(geom_errors))


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
        if 'start' in request.keys():
            start = request['start']
        else: 
            start = 0
        
        process_wfs(request['region'],
                    size,
                    request['url'],
                    request['params'],
                    request['count'],
                    start=start, 
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

    if "zip" in request['jobs']:
        process_zip(request['region'],
                    request['url'],
                    request['path_data'])
    
    if "xml" in request['jobs']:
        process_xml(request['region'],
                    request['url'],
                    request['path_data'],
                    request['params'])

    if 'parquet' in request['jobs']:
        safe_parquet(request['region'],
                    request['path_data'],
                    request['params'])


if __name__ == "__main__":
    main() 

