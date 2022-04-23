import pandas as pd
import numpy as np
import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time
import os.path
import httplib2
from bs4 import BeautifulSoup, SoupStrainer


def download_url(url, save_path, chunk_size=128):
    """
    Function to download urls
    """
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


http = httplib2.Http()
status, response = http.request('http://opendata.regione.abruzzo.it/content/dbtr-regione-abruzzo-scala-15000-edizione-2007-formato-shp')

# Get List of download links
link_list = []
for link in BeautifulSoup(response,'html.parser', parse_only=SoupStrainer('a')):
    if link.has_attr('href'):
        if link['href'].endswith('.rar'):
            link_list.append(link['href'])
            #print(link['href'])
            

# loop over all links to downlaod the zip files
path_folder = '/Volumes/ssd_500gb/Drive/Downloads/italy_abruzzo'
i=0
for lnk in link_list:
    i = i+1
    #print(lnk)
    path = os.path.join(path_folder,lnk.rsplit('/', 1)[1])
    # takes path + url string and takes second part of split at last '/' to define output file name
    #path = os.path.join(path_folder,df['3DShape'][i].rsplit('/', 1)[1])
    # download 
    download_url(lnk,path)
    print('downloaded: {} of {} files '.format(i, len(link_list)))
