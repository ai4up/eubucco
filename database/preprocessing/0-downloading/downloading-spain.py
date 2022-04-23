import requests
from lxml import etree
import urllib.parse
import urllib.request
import sys
import os
import time


start = time.time()

path_output = '/data/metab/EU-buildings-energy/data/raw-data/3d-models/spain-cadaster'

url_main_page = 'http://www.catastro.minhap.es/INSPIRE/buildings/ES.SDGC.BU.atom.xml'

# fetch xml from url
response = requests.get(url_main_page)
# read in content in bytes
content = response.content
# create a etree object
root = etree.fromstring(content)

# fetch the urls to all regions xml
list_regions_urls = root.findall('{http://www.w3.org/2005/Atom}entry/{http://www.w3.org/2005/Atom}id')
# fetch the names of all regions
list_region_names = root.findall('{http://www.w3.org/2005/Atom}entry/{http://www.w3.org/2005/Atom}title')

for i in range(len(list_regions_urls)):

	# example: "Territorial office 02 Albacete"
	region_name = list_region_names[i].text.split(" ")[-1]
	print("-------------")
	print(region_name)
	print("-------------")

	# create folder
	path_region_folder = os.path.join(path_output,region_name)
	if not os.path.exists(path_region_folder):
   		os.mkdir(path_region_folder)

	# get the xml of the region
	url_region = list_regions_urls[i].text
	response = requests.get(url_region)
	content = response.content
	root = etree.fromstring(content)

	# get all the urls to individual cities
	list_cities_urls = root.findall('{http://www.w3.org/2005/Atom}entry/{http://www.w3.org/2005/Atom}id')
	list_cities_names = root.findall('{http://www.w3.org/2005/Atom}entry/{http://www.w3.org/2005/Atom}title')

	for j in range(len(list_cities_urls)):

		# example: "03002-AGOST buildings"
		city_name = list_cities_names[j].text[:-10]
		print(city_name)

		path_output_file = os.path.join(path_region_folder, city_name+'.zip')

		# making sure the spaces in the urls because of city names in several parts dont break anything 
		url_download = requests.utils.requote_uri(list_cities_urls[j].text)

		# download
		urllib.request.urlretrieve(url_download,path_output_file)


end = time.time()
last = divmod(end - start, 60)
print('Downloaded in {} minutes {} seconds'.format(last[0],last[1])) 
