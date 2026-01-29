import duckdb
import os

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

# Resource management
tmp_dir = "/p/projects/eubucco/data/tmp/duckdb_temp"
os.makedirs(tmp_dir, exist_ok=True)
con.execute(f"SET temp_directory='{tmp_dir}'")
con.execute("SET memory_limit='64GB'")

input_pattern = "/p/projects/eubucco/data/7-release/*.parquet"
output_file = "/p/projects/eubucco/data/8-additional-files/eubucco_lat_lon.parquet"

# Combine all Parquet files, enforce schema, calculate lat/lon, and export to final Parquet
query = f"""
COPY (
    SELECT 
        id::VARCHAR as id,
        region_id::VARCHAR as region_id,
        city_id::VARCHAR as city_id,
        type::VARCHAR as type,
        subtype::VARCHAR as subtype,
        height::DECIMAL(4,1) as height,
        floors::DECIMAL(4,1) as floors,
        construction_year::SMALLINT as construction_year,
        
        type_confidence::DECIMAL(3,2) as type_confidence,
        subtype_confidence::DECIMAL(3,2) as subtype_confidence,
        height_confidence_lower::DECIMAL(4,1) as height_confidence_lower,
        height_confidence_upper::DECIMAL(4,1) as height_confidence_upper,
        floors_confidence_lower::DECIMAL(4,1) as floors_confidence_lower,
        floors_confidence_upper::DECIMAL(4,1) as floors_confidence_upper,
        construction_year_confidence_lower::SMALLINT as construction_year_confidence_lower,
        construction_year_confidence_upper::SMALLINT as construction_year_confidence_upper,

        geometry_source::VARCHAR as geometry_source,
        type_source::VARCHAR as type_source,
        subtype_source::VARCHAR as subtype_source,
        height_source::VARCHAR as height_source,
        floors_source::VARCHAR as floors_source,
        construction_year_source::VARCHAR as construction_year_source,

        geometry_source_id::VARCHAR as geometry_source_id,
        type_source_ids::VARCHAR[] as type_source_ids,
        subtype_source_ids::VARCHAR[] as subtype_source_ids,
        height_source_ids::VARCHAR[] as height_source_ids,
        floors_source_ids::VARCHAR[] as floors_source_ids,
        construction_year_source_ids::VARCHAR[] as construction_year_source_ids,

        subtype_raw::VARCHAR as subtype_raw,

        -- Calculated Lat/Lon
        ST_Y(ST_Transform(ST_Centroid(geometry), 'EPSG:3035', 'EPSG:4326'))::DOUBLE AS lat,
        ST_X(ST_Transform(ST_Centroid(geometry), 'EPSG:3035', 'EPSG:4326'))::DOUBLE AS lon

    FROM read_parquet('{input_pattern}')
    ORDER BY geometry_source, region_id, city_id, id
) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 10000);
"""
        
con.execute(query)
print(f"Success. EUBUCCO release with lat lon created: {output_file}")