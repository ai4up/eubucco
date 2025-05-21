import os 
import sys
import json
import re
from glob import glob

import geopandas as gpd

def get_overview(gdf):
    overview = {}
    
    # get info about shape
    overview['shape'] = gdf.shape
    
    # get relevant info and examples about columns
    overview['column_examples'] = {}
    for col in gdf.drop(columns='geometry').columns:
        overview['column_examples'][col] = gdf[col].iloc[0:2].astype(str).to_list()

    # get info about geometry types
    overview['geom_types'] = set(gdf.geometry.geom_type)
    overview['has_z'] = gdf.geometry.has_z.any()
    if overview['has_z']:
        overview['head_geoms'] = gdf.geometry.head()

    # check for duplicates
    duplicate_count = {}
    for idx in ['gml_id','oid','id']:
        if idx in gdf.columns:
            duplicate_count[idx] = len(gdf[gdf.duplicated(subset=[idx], keep=False)])
    
    overview['duplicate_count'] = duplicate_count
    return overview


def _create_overview_dir(path_dir, region):
    path_dir_overview = os.path.join(path_dir, 'overview')
    if not os.path.exists(path_dir_overview):
        os.makedirs(path_dir_overview)
    return os.path.join(path_dir_overview,f'{region}_overview.txt')


def save_to_txt(path_dir, region, overview):
    
    path_overview = _create_overview_dir(path_dir, region)

    with open(os.path.join(path_overview), 'w') as f:
        f.write(f'Overview of {region}\n')
        f.write('------------------------\n\n')
        f.write(f"gdf{overview['shape']}:\n")
        for col, ex in overview['column_examples'].items():
            f.write(f"  {col}: {', '.join(ex)}\n")
        f.write('\n\nGeometry types:\n')
        f.write(str(overview['geom_types']))
        if overview['has_z']:
            f.write('\n\nContains Z geometry\n')
            f.write(overview['head_geoms'].to_string(index=False))
        if overview['duplicate_count']:
            f.write('\n\nContains duplicates:\n')
            f.write(str(overview['duplicate_count']))


def get_input():
    request = json.load(sys.stdin)
    print(request)
    return request


def main():
     
    request = get_input()
    gdf = gpd.read_parquet(os.path.join(request['path_dir'],
                            request['country'],
                            f'buildings_{request["region"]}.pq'))

    save_to_txt(request['path_dir'], request['region'], get_overview(gdf))


if __name__ == "__main__":
    main() 