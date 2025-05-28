import os 
import sys
import json
import re
import time
from glob import glob
from io import BytesIO
import zipfile
import pathlib
import xml.etree.ElementTree as ET

import fiona
import pandas as pd
import geopandas as gpd
from shapely import wkb
import requests
import urllib.parse


def _get_size(url, params_tmp):
    params_tmp['resultType'] = 'hits'
    response = requests.get(url, params=params_tmp)
    
    if response.status_code == 200:
        size = re.findall(r'numberMatched="[0-9]+"', str(response.content))
        size = int(size[0].split('=')[-1][1:-1])
        return size
    return 'unknown'


def process_wfs(region,
                size,
                url,
                params,
                count,
                path_data,
                start=0):
    
    size = _get_size(url, params.copy())
    print(f"Number of files: {size}")

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
            
        gdf.to_parquet(os.path.join(path_data, "raw",f"buildings_{region}_{i}_raw.pq"))


def download_meta4(region, url, path_data):
    """
    Downloads a meta4 file and returns a list of tuples with file names and URLs.
    """
    print("creating zip dir (for meta4)")
    path_zip_dir = os.path.join(path_data, 'raw',f'zip_{region}')
    pathlib.Path(path_zip_dir).mkdir(exist_ok=True)
    
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to download meta4 file from {url}")
        raise ValueError(f"Failed to download meta4 file from {url}")

    with open(os.path.join(path_zip_dir,'meta4.xml'), 'wb') as f:
        f.write(response.content)
    print(f"Meta4 file downloaded to {os.path.join(path_zip_dir,'meta4.xml')}")


def _parse_meta4(file_path):
    ns = {'m': 'urn:ietf:params:xml:ns:metalink'}
    tree = ET.parse(file_path)
    root = tree.getroot()
    files = []
    for file_elem in root.findall('m:file', ns):
        name = file_elem.get('name')
        urls = [ue.text for ue in file_elem.findall('m:url', ns)]
        files.append((name, urls))
    return files  


def _download_read_gml(name, url,path_data, region):
    resp = requests.get(url)
    file_path = os.path.join(path_data,'raw',f'zip_{region}', f"{name.rsplit('.')[0]}.gml")
    with open(file_path, "wb") as f:
        f.write(resp.content)
    
    layers = fiona.listlayers(file_path)
    print(f"Found layers: {layers} in {file_path}")


def process_meta4(region, path_data):
    files = _parse_meta4(os.path.join(path_data, 'raw',f'zip_{region}', 'meta4.xml'))
    i=0
    for name, urls in files:
        try: 
            _download_read_gml(name, urls[0], path_data, region)
        except Exception as e:
            print(f"Error downloading {name}: {e}. Testing second URL if available.")
            if len(urls) > 1:
                try: _download_read_gml(name, urls[1], path_data, region)
                except Exception as e:
                    print(f"Error downloading {name} from second URL: {e}")
        
        # if gdf is not None:
        #     i+=1
        #     gdf.to_parquet(os.path.join(path_data,'raw', f"buildings_{region}_{name.rsplit('.')[0]}_raw.pq"))
        if i>100:
            print("Stopping after 100 files for testing.")
            break
    
    print(f"Processed and saved {i} of {len(files)} files from meta4 for {region}.")    
    

def process_json(region,
                url_prefix,
                params,
                count,
                path_data):
    
    for i in range(params['start'], params['stop'], count):
        url = url_prefix+str(i)+params['url_postfix'] # construct json url
        gdf = gpd.read_file(url)
        print(f"{i}: {len(gdf)} Buildings from {url.rsplit('/',1)[1]}")
        gdf.to_parquet(os.path.join(path_data,'raw', f"buildings_{region}_{i}_raw.pq"))


def process_json_w_gpkg(region,
                        url,
                        path_data):
    
    links = gpd.read_file(url)
    for i,link in enumerate(links.zip.values):
        link = urllib.parse.quote(link, safe=':/')
        gdf = gpd.read_file(link, layer='gebaeude')
        print(f"{i}: {len(gdf)} Buildings from {link.split('/')[-1][:-9]} (special case info)")
        gdf.to_parquet(os.path.join(path_data,'raw', f"buildings_{region}_{i}_raw.pq"))


def _parse_atom_xml(atom_xml,
                     params):
    """
    Parses the Atom XML and returns every <link> 
    whose type is a zipped LoD2 package.
    """
    root = ET.fromstring(atom_xml)
    for link in root.findall('.//atom:entry/atom:link', params):
        href = link.get('href', '')
        mime = link.get('type', '')
        # application/x-gmz or .zip extension both indicate our zips
        if mime == 'application/x-gmz' or href.lower().endswith('.zip'):
            yield href


def _get_zip_links(params):
    # for edge case in Berlin, where urls to zip files first needed 
    # to be extracted from an atom xml
    response = requests.get(params['feed_url'])
    response.raise_for_status()
    
    urls = []
    for url in _parse_atom_xml(response.text, params['namespaces']):
        urls.append(url)
    return urls


def _list(x):
    if isinstance(x, str): return [x]
    else: return x


def process_zip(region,
                urls,
                params,
                path_data):
    
    print("creating zip dir")
    path_zip_dir = os.path.join(path_data, 'raw',f'zip_{region}')
    pathlib.Path(path_zip_dir).mkdir(exist_ok=True)
    
    if urls is None:
        urls = _get_zip_links(params) 

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


def process_xml(region,
                url,
                params,
                path_data):
    
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
        gdf.to_parquet(os.path.join(path_data, 'raw', f'buildings_{region}_{code}_raw.pq'))


def _read_gml_files(file, region, params):
    # hamburg requires specific layer name
    if "layer" in params.keys():
        gdf = gpd.read_file(file, layer=params['layer'])
        if "crs" in params.keys():
            gdf = gdf.set_crs(params['crs'])
        return gdf
    else:
        # tries to read gml or xml with fiona first to avoid piogrio error for wkb type 15
        try:
            return gpd.read_file(file, engine='fiona')
        except:
            try:
                return gpd.read_file(file, engine='pyogrio')
            except:
                try:
                    time.sleep(0.5) # waiting 0.5 seconds ensures creation of gds meta file which in some cases is needed by fiona to process gml files
                    return gpd.read_file(file, engine='fiona')
                except:
                    print(f'GML ERROR. Could not read: {file}')
                    return gpd.GeoDataFrame()


def _read_geofile(file, region, params):
    if pathlib.Path(file).suffix == ".pq":
        return gpd.read_parquet(file)
    elif pathlib.Path(file).suffix == ".xml" or pathlib.Path(file).suffix == ".gml":
        return _read_gml_files(file, region, params)
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
    for id_x in ['gml_id', 'oid', 'id', 'objid']:
        if id_x in gdf.columns:
            # remove duplicates based on id_x column
            gdf = gdf[~gdf[id_x].duplicated()]
    return gdf


def _clean_individual_gdf(gdf):
    # turning columns with lists into strings to avoid issues with parquet
    for col in gdf.columns:
        if col in gdf.columns[gdf.dtypes=='object']:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    return gdf


def _check_geometry_types(gdf):    
    invalid_geometries = gdf[~gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])]
    invalid_types = list(set(invalid_geometries.geometry.geom_type))

    if gdf.geometry.has_z.any():
        invalid_types.append('z_geometry')

    if not invalid_geometries.empty:
        print("Found geometries that are not 'Polygon' or 'MultiPolygon':")
        print(invalid_types)
        return invalid_types
    else:
        print("Check successful: All geometries are either 'Polygon' or 'MultiPolygon'.")
        return None


def _fix_invalid_geometries(gdf, invalid_types):
    if "MultiLineString" in invalid_types:
        n_invalid_geoms = len(gdf.loc[gdf.geometry.geom_type == 'MultiLineString'])
        gdf.loc[gdf.geometry.geom_type == "MultiLineString",'geometry'] = gdf.loc[gdf.geometry.geom_type == "MultiLineString",'geometry'].polygonize()
        print(f"Fixed {n_invalid_geoms} MulitLineString geometries.")
    if "z_geometry" in invalid_types:
        n_invalid_geoms = len(gdf.loc[gdf.geometry.has_z])
        gdf.loc[gdf.geometry.has_z,'geometry'] = gdf.loc[gdf.geometry.has_z,'geometry'].force_2d()
        print(f"Fixed {n_invalid_geoms} Z geometries.")
    return gdf


def _clean_concatenated_gdf(gdf):
    gdf = _remove_id_duplicates(gdf)
    invalid_types = _check_geometry_types(gdf)
    if invalid_types:
        gdf = _fix_invalid_geometries(gdf, invalid_types)
    return gdf


def safe_parquet(region, params, path_data):
    if params is not None:
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
            gdf = _read_geofile(file, region, params)
            if gdf.crs is None:
                print('Warning! CRS not available; Setting crs')
                gdf.set_crs(params['crs'], inplace=True)
            
        except ValueError:
           data_errors, geom_errors = _handle_missing_metadata(file,
                                                                data_errors,
                                                                geom_errors)

        if gdf.shape[0]:
            gdf = _clean_individual_gdf(gdf)
            print(f"appending geoms of file:{file}")
            frames.append(gdf.to_crs(epsg=3035))
    
    gdf = pd.concat(frames, ignore_index=True) 

    gdf = _clean_concatenated_gdf(gdf)

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
    
    path_data = os.path.join(request['path_dir'],request['country'])
    init(path_data)

    if 'wfs' in request['jobs']:
        if 'start' in request.keys(): start = request['start']
        else: start = 0
        
        process_wfs(request['region'],
                    request['url'],
                    request['params'],
                    request['count'],
                    path_data,
                    start=start)

    if "json" in request['jobs']:
        process_json(request['region'],
                    request['url'],
                    request['params'],
                    request['count'],
                    path_data)
    
    if "json_w_gpkg" in request['jobs']:
        process_json_w_gpkg(request['region'],
                    request['url'],
                    path_data)

    if "zip" in request['jobs']:
        process_zip(request['region'],
                    request['url'],
                    request['params'],
                    path_data)
    
    if "xml" in request['jobs']:
        process_xml(request['region'],
                    request['url'],
                    request['params'],
                    path_data)
    
    if "download_meta4" in request['jobs']:
        download_meta4(request['region'],
                       request['url'],
                       path_data)
        
    if "meta4" in request['jobs']:
        process_meta4(request['region'],
                      path_data)

    if 'parquet' in request['jobs']:
        safe_parquet(request['region'],
                    request['params'],
                    path_data)


if __name__ == "__main__":
    main() 

