# Downloading building data

The first step of the workflow is to download building data.

## OpenStreetMap

OpenStreetMap data is accessible on [geofabrik](https://download.geofabrik.de/). We use the great library `pyrosm` to automate the downloads. We download at the country level or sub-country regional level when these are available for the largest countries in the EU. The data are saved in the OSM `osm.pbf` format. One can download all the files at once by running the code from the notebook `downloading-openstreetmap.ipynb`. See a tutorial HERE.

## Open governmental data

Open governmental data can be downloaded through different procedures depending on the juridiction, ranging from simple clicks for the whole to webscrappers. All the download approaches are listed in the [raw data info sheet](https://docs.google.com/spreadsheets/d/1O5n603RWsJLAHjMlmuTu7_hnfZJBD05PuurOCPZgRSw/edit#gid=0). In this folder, we keep all scripts that have been used to download data. 

## Unzipping files

Files are unzipped on the cluster using the follow commands in the terminal:

```
unzip <file> -d <folder where to unzip> -> one zip file for an area 

unzip <file> "path/to/specific/data/*" -> avoiding uncessary files

unzip "*.zip" -> unzip all zips in a folder e.g. when data come as tiles/cities

```

