import os 
import sys
import json
import yaml
import re
import time
from glob import glob
from io import BytesIO
import zipfile
import pathlib
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkb
from shapely.geometry import Polygon, MultiPolygon
import requests
import urllib.parse

from lxml import etree
from shapely.geometry import Polygon

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


def _download_gml(name, url,path_data, region):
    resp = requests.get(url)
    file_path = os.path.join(path_data,'raw',f'zip_{region}', f"{name.rsplit('.')[0]}.gml")
    with open(file_path, "wb") as f:
        f.write(resp.content) 
  

def process_meta4(region, path_data):
    files = _parse_meta4(os.path.join(path_data, 'raw',f'zip_{region}', 'meta4.xml'))
    i=0
    for name, urls in files:
        try: 
            _download_gml(name, urls[0], path_data, region)
            i += 1
        except Exception as e:
            print(f"Error downloading {name}: {e}. Testing second URL if available.")
            if len(urls) > 1:
                try: _download_gml(name, urls[1], path_data, region)
                except Exception as e:
                    print(f"Error downloading {name} from second URL: {e}")
                    i -= 1  # do not count this file as processed
    
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


def process_json_w_gml(region,
                        url,
                        path_data):
    
    print("creating zip dir (for json with gml links)")
    path_zip_dir = os.path.join(path_data, 'raw',f'zip_{region}')
    pathlib.Path(path_zip_dir).mkdir(exist_ok=True)

    links = gpd.read_file(url)
    for i,link in enumerate(links.xml.values):
        url = urllib.parse.quote(link, safe=':/')
        print('Processing:',url)
        name = url.rsplit('/',1)[1]
        _download_gml(name, url,path_data, region)


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


def _get_zip_links(params, path_data):
    # for edge case in Berlin, where urls to zip files first needed 
    # to be extracted from an atom xml
    if 'feed_url' in params.keys():
        response = requests.get(params['feed_url'])
        response.raise_for_status()
    elif 'feed_file_path' in params.keys():
        with open(os.path.join(path_data,params['feed_file_path']), 'r') as f:
            response = yaml.safe_load(f)
        return response['urls']
    else:
        raise ValueError("No feed URL or file path provided in params.")
        
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
        urls = _get_zip_links(params, path_data) 

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


def _extract_ogc_metadata(node, ns):
    record = {}

    # creationDate, measuredHeight, roofType
    record["creationDate"] = node.findtext("core:creationDate", namespaces=ns)
    record["roofType"] = node.findtext("bldg:roofType", namespaces=ns)
    record["function"] = node.findtext("bldg:function", namespaces=ns)
    record["storeysAboveGround"] = node.findtext("bldg:storeysAboveGround", namespaces=ns)

    mh_node = node.find("bldg:measuredHeight", namespaces=ns)
    if mh_node is not None:
        record["measuredHeight"] = mh_node.text
        record["measuredHeight_uom"] = mh_node.attrib.get("uom")

    # Dynamically collect all generic attributes
    for tag in ["stringAttribute", "dateAttribute", "intAttribute", "doubleAttribute"]:
        for attr in node.findall(f"gen:{tag}", namespaces=ns):
            key = attr.attrib.get("name")
            val = attr.findtext("gen:value", namespaces=ns)
            if key:
                record[key] = val

    return record


def _extract_ogc_groundsurface_polygon(xmlnode, ns):
    ground_surface = xmlnode.find(".//bldg:GroundSurface", namespaces=ns)
    if ground_surface is not None:
        poslist = ground_surface.find(".//gml:posList", namespaces=ns)
        if poslist is not None and poslist.text:
            coords = list(map(float, poslist.text.strip().split()))
            coords2d = [(coords[i], coords[i+1]) for i in range(0, len(coords), 3)]
            return Polygon(coords2d)
    return None


def extract_ogc_building_and_parts(tree, crs="EPSG:25832", ns=None):
    buildings = tree.findall(".//bldg:Building", namespaces=ns)
    records = []
    for b in buildings:
        building_id = b.attrib.get("{http://www.opengis.net/gml}id")
        parts = b.findall(".//bldg:BuildingPart", namespaces=ns)

        # building has parts: collect parts and create one building geom out of its
        if parts:
            for part in parts:
                part_id = part.attrib.get("{http://www.opengis.net/gml}id")
                record = _extract_ogc_metadata(part, ns)
                record["id"] = f"{building_id}_{part_id}" # newly created to support gml inlc building parts
                record["building_id"] = building_id
                record["part_id"] = part_id
                record["geometry"] = _extract_ogc_groundsurface_polygon(part, ns)
                records.append(record)

        # building has no parts: handle as flat building
        else:
            record = _extract_ogc_metadata(b, ns)
            record["id"] = f"{building_id}"
            record["building_id"] = building_id
            record["part_id"] = None
            record["geometry"] = _extract_ogc_groundsurface_polygon(b, ns)
            records.append(record)

    return gpd.GeoDataFrame(records, geometry="geometry", crs=crs)


def _extract_inspire_metadata(node, ns, type):
    rec = {"type": type}
    rec["id"] = node.attrib.get("{http://www.opengis.net/gml/3.2}id")
    rec["gml_identifier"] = node.findtext("gml:identifier", namespaces=ns)
    rec["beginLifespanVersion"] = node.findtext("bu-base:beginLifespanVersion", namespaces=ns)
    rec["numberOfFloorsAboveGround"] = node.findtext("bu-base:numberOfFloorsAboveGround", namespaces=ns)

    # Inspire ID
    rec["inspire_localId"] = node.findtext("bu-base:inspireId/base:Identifier/base:localId", namespaces=ns)
    rec["inspire_namespace"] = node.findtext("bu-base:inspireId/base:Identifier/base:namespace", namespaces=ns)

    uses = node.findall("bu-base:currentUse/bu-base:CurrentUse/bu-base:currentUse", namespaces=ns)
    
    if uses:
        hrefs = [u.attrib.get("{http://www.w3.org/1999/xlink}href") for u in uses if u is not None]
        hrefs = [h for h in hrefs if h is not None]  # Filter out None values
        rec["currentUses"] = ";".join(hrefs)
    return rec


def _extract_inspire_geometry(node, ns):
    """
    Extracts 2D geometry from:
    - gml:Polygon
    - gml:MultiSurface (with multiple gml:surfaceMember/gml:Polygon)
    """
    pos_lists = node.findall(".//bu-base:geometry//gml:posList", namespaces=ns)
    polygons = []

    for poslist in pos_lists:
        if poslist.text:
            coords = list(map(float, poslist.text.strip().split()))
            coords2d = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]
            poly = Polygon(coords2d)
            if poly.is_valid:
                polygons.append(poly)

    if not polygons:
        return None
    elif len(polygons) == 1:
        return polygons[0]
    else:
        return MultiPolygon(polygons)


def extract_inspire_building_and_parts(tree, crs="EPSG:25832", ns=None):
    records = []
    # found building parts: handle parts as individual building 
    parts = tree.findall(".//bu-core2d:BuildingPart", namespaces=ns)
    for part in parts:
        rec = _extract_inspire_metadata(part, ns, type="BuildingPart")
        rec["geometry"] = _extract_inspire_geometry(part, ns)
        records.append(rec)

    # found individual buildings: handle as flat building
    buildings = tree.findall(".//bu-core2d:Building", namespaces=ns)
    for b in buildings:
        rec = _extract_inspire_metadata(b, ns, type="Building")
        rec["geometry"] = _extract_inspire_geometry(b, ns)
        records.append(rec)

    return gpd.GeoDataFrame(records, geometry="geometry", crs=crs)


def _read_gml_files(file, region, params):
    # parse gml with self-build parser that can handle both buildings and building parts, incl geoms and respective attributes
    # if parser fails, fall back to geopandas read_file
    if "city_gml_type" in params.keys():
        tree = etree.parse(file)
        if params['city_gml_type'] == 'inspire':
            return extract_inspire_building_and_parts(tree, crs=params['crs'], ns=params['namespaces'])
        else:
            return extract_ogc_building_and_parts(tree, crs=params['crs'], ns=params['namespaces'])
    else:
        try:
            # some cases require layer to be specified as some gml files might be corrupted and fiona cannot find the right layer
            if "layer" in params.keys(): layer = params['layer']
            else: layer = None
            return gpd.read_file(file, layer=layer)
        except:
            print(f'GML ERROR. Could not read: {file}')
            return gpd.GeoDataFrame()


def _read_geofile(file, region, params):
    if pathlib.Path(file).suffix == ".pq":
        return gpd.read_parquet(file)
    elif pathlib.Path(file).suffix == ".xml" or pathlib.Path(file).suffix == ".gml":
        return _read_gml_files(file, region, params)
    else:
        try: 
            return gpd.read_file(file,engine='pyogrio')
        except:
            return gpd.read_file(file,engine='fiona')


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


def _check_crs(gdf,params):
    if (gdf.crs is None) & ('crs' in params.keys()):
        print(f"Warning! CRS not available; Setting crs to {params['crs']}")    
        gdf.set_crs(params['crs'], inplace=True)
    return gdf


def _select_relevant_rows(gdf, params):
    if 'filter' in params.keys():
        for column, value in params['filter'].items():
            gdf = gdf[gdf[column]==value]
        gdf.reset_index(drop=True, inplace=True)
    return gdf


def _clean_individual_gdf(gdf,params):
    gdf = _select_relevant_rows(gdf, params)
    gdf = _check_crs(gdf,params)
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

    if (not invalid_geometries.empty) or (len(invalid_types) > 0):
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


# def remove_ndarray_columns(gdf):
#     array_cols = [col for col in gdf.columns if gdf[col].apply(lambda x: isinstance(x, np.ndarray)).any()]
#     if array_cols:
#         for col in array_cols:
#             print(f"Removing ndarray values in {col}")
#             gdf[col] = gdf[col].astype(str)  # Convert ndarray columns to string
#     return gdf


def _harmonize_none_entries(gdf):
    null_strings = [None,'None', 'NONE','nan', 'NaN','null', 'NULL','n/a', 'N/A',''                 ]
    gdf = gdf.replace(null_strings, np.nan)  # Replace None with NaN
    return gdf


def _prepare_dtypes_for_paruquet(gdf):
    """
    Saving in paruet requires compatible dtypes.
    It can happen that parquet does not infer the right dtypes, leaving certain
    columns as object dtype, which causes pyarrow.lib.ArrowTypeError.
    This functions infers dtype first, then converts leftover object dtypes to StringDtype
    
    Note that this is not perfect, as it causes different NaN representations: dtype float64 will
    have np.nan, while dtype string[python] will have pd.NA. For any further processing, this should
    not be an issue, though, as both represent missing values.
    """

    gdf = gdf.convert_dtypes(
            infer_objects=True,
            convert_string=True,
            convert_integer=False,
            convert_boolean=False,
            convert_floating=False,
            dtype_backend="numpy_nullable")
    
    # Cast non inferrable dtype objects to pandas StringDtype
    obj_cols = gdf.select_dtypes(include=["object"]).columns
    
    for col in obj_cols:
        gdf[col] = gdf[col].astype("string")
    return gdf


def _clean_concatenated_gdf(gdf):
    gdf = _remove_id_duplicates(gdf)
    
    invalid_types = _check_geometry_types(gdf)
    if invalid_types:
        gdf = _fix_invalid_geometries(gdf, invalid_types)

    gdf = _harmonize_none_entries(gdf)
    gdf = _prepare_dtypes_for_paruquet(gdf)
    return gdf


def safe_parquet(region, params, path_data):
    if "file_type" in params.keys():
        # for zip cases - file types can be different per state and must be specified in params
        path_files = os.path.join(path_data, 'raw',f"zip_{region}/**/*.{params['file_type']}")
    else:
        path_files = os.path.join(path_data, 'raw',f'buildings_{region}*')
    
    files = glob(path_files,recursive=True)
    frames = []
    data_errors = []
    geom_errors = []
    
    i=0
    missed_files = []
    for file in files:
        try: 
            gdf = _read_geofile(file, region, params)
        except ValueError:
           data_errors, geom_errors = _handle_missing_metadata(file,
                                                                data_errors,
                                                                geom_errors) #TODO move into reading parquet file
        
        if gdf.shape[0]:
            print(f"appending geoms of file:{file}")
            gdf = _clean_individual_gdf(gdf,params)
            frames.append(gdf.to_crs(epsg=3035))
            i += 1
        else:
            missed_files.append(file)
    
    gdf = pd.concat(frames, ignore_index=True) 

    gdf = _clean_concatenated_gdf(gdf)

    print("-"*12)
    print("Run summary:")
    print("buildings processed: ", len(gdf))
    print("building columns: ", gdf.columns)
    print("building crs: ", gdf.crs)
    print(f"concatenaed buildings from {i} our of {len(files)} files")
    if len(missed_files) > 0:
        print(f"Files which are empty (or could not be opened): {len(missed_files)}")
        print("\n".join(missed_files))
    print("-"*12)
    
    if len(data_errors) > 0:    
        print("*"*8)
        print(f"Found {len(data_errors)} Metadata errors in following files:")
        print("\n".join(data_errors))
        print("*"*8)
        print(f"Found {len(geom_errors)} Missing geom errors in following files:")
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
    
    if "json_w_gml" in request['jobs']:
        process_json_w_gml(request['region'],
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

