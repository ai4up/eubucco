import os
import time
import osmnx as ox
import momepy

from ufo_map.Preprocessing.preproc_streets import *
from ufo_map.Utils.helpers import import_csv_w_wkt_to_gdf,save_csv_wkt,get_all_paths,get_stats,arg_parser
from preproc.db_set_up import fetch_GADM_info_country

CRS_UNI = 'EPSG:3035'

def download_osm_streets(country_name,
                         path_stats='/p/projects/eubucco/stats/6-streets'):
    '''
        Downloads OpenStreetMap street network for a country at a city-level and saves a city-level
        <city>_streets.csv file in the relevant city-level folder with OSM edges encoded as WKT strings.

        Requires an appropriate gadm_table.csv file and folder structure + boundary files previously created in db set-up.

        Returns: None
    '''

    print(country_name)

    paths_in = get_all_paths(country_name,'/p/projects/eubucco/data/2-database-city-level-v0_1','boundary')
    paths_out = get_all_paths(country_name,'/p/projects/eubucco/data/2-database-city-level-v0_1','streets')

    n_streets = 0

    start = time.time()

    for path_in,path_out in zip(paths_in,paths_out):

        print(path_out)

        boundary = import_csv_w_wkt_to_gdf(path_in,CRS_UNI,geometry_col='boundary_GADM_2k_buffer').to_crs(4326).geometry.iloc[0]

        city_network = ox.graph_from_polygon(boundary, 
                                         simplify=True, 
                                         network_type='drive')

        city_streets = ox.utils_graph.graph_to_gdfs(city_network, 
                                       nodes=False, 
                                       edges=True,
                                       node_geometry=False, 
                                       fill_edge_geometry=True).to_crs(CRS_UNI)[['osmid','highway','length','geometry']]

        n_streets += len(city_streets)

        save_csv_wkt(city_streets,path_out)

    end = time.time() - start

    get_stats({'country_name':country_name,'n_streets':n_streets},end,path_stats,country_name,'stat_ddl')



def create_streets_and_intersections(streets,buildings,path_str,path_int):
    '''
        Modifies and saves in the relevant folder city-level street file from street linestrings.

        Returns: None 
    '''

    streets = momepy.gdf_to_nx(streets)

    print('Creating intersections...')
    intersections = momepy.nx_to_gdf(streets, lines=False)
    save_csv_wkt(intersections,path_int)
    len_intersections = len(intersections)
    del intersections

    print('Adding centrality metrics to streets...')
    streets = momepy.betweenness_centrality(streets,mode='edges',name='betweenness_metric_e')
    streets = momepy.closeness_centrality(streets,name='closeness_global')
    streets = momepy.closeness_centrality(streets,radius=500,name='closeness500',distance='mm_len',weight='mm_len')
    streets = network_to_street_gdf(streets,buildings)
    
    return(streets, len(streets),len_intersections)



def create_sbb(streets,path_sbb):
    '''
        Creates and saves in the relevant folder city-level street-based blocks file from street linestrings.

        Needs to be run on a clean street file created through the ufo-map `rm_duplicates_osm_streets` function,
        here called within `create_streets_and_intersections`.

        Returns: None 
    '''
    print('Creating street-based blocks...')
    sbb = generate_sbb(streets)
    save_csv_wkt(sbb,path_sbb)
    return(len(sbb))



def parse_streets(country_name,left_over=False):
    '''
        Parses Openstreetmap street network to create an updated <city>_streets.csv with computed network centrality metrics,
        a <city>_intersections.csv file and a <city>_sbb.csv with street-based blocks created by polygonizing the street linestrings.

        Requires city-level OSM street network from `download_osm_streets`.

        Returns: None
    '''

    args = arg_parser(['i'])
    print(args.i)

    start = time.time()

    paths = {}
    for file in ['streets','geom','intersections','sbb']:
        if left_over==False:
            paths[file] = get_all_paths(country_name,'/p/projects/eubucco/data/2-database-city-level-v0_1',file)[args.i]
        else:
            paths[file] = get_all_paths(country_name,'/p/projects/eubucco/data/2-database-city-level-v0_1',file,left_over)[args.i]

    path_stats = os.path.join('/p/projects/eubucco/data/2-database-city-level',
                              country_name,
                              'stats-parts-str')
 
    streets = import_csv_w_wkt_to_gdf(paths['streets'],CRS_UNI)
    buildings = import_csv_w_wkt_to_gdf(paths['geom'],CRS_UNI)

    streets, n_streets, n_int = create_streets_and_intersections(streets,buildings,paths['streets'],paths['intersections']) 

    n_sbb = create_sbb(streets,paths['sbb'])

    save_csv_wkt(streets,paths['streets'])

    end = time.time() - start    

    get_stats({'city_idx':args.i,'n_streets':n_streets,'n_int':n_int,'n_sbb':n_sbb},end,path_stats,str(args.i),'stat_str')
