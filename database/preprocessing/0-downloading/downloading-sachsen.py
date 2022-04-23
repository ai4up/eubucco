# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 13:19:45 2021

@author: jiawe
"""

import requests 
from lxml import etree
import os

url='http://geo.data.linz.gv.at/katalog/geodata/3d_geo_daten_lod2/2018' 
responce=requests.get(url) 
responce.encoding='gbk' 
content=responce.text 

html=etree.HTML(content) 

urls=html.xpath('//a/@href') # Get the attribute value of href in a  
tags=html.xpath('//a/text()') #Get text of a 
print(urls[:10],tags[:10])

urls_all=[]
for i in range(len(urls)):
 urls_all.append('http://geo.data.linz.gv.at/katalog/geodata/3d_geo_daten_lod2/2018/'+urls[i]) 
print(urls_all[5:]) 

os.makedirs('Linz') 
os.chdir(r'C:\Users\jiawe\Documents\Nicola project\file\learning-from-urban-form-to-predict-building-heights-master\sample-data\1-parsing-citygml\Linz') 
for i in range(len(urls_all)): 
    res = requests.get(urls_all[i])
    print('runing in {0}，all{1}'.format(str(i + 1), str(len(urls_all)))) 
    with open('{}.gml'.format(str(tags[i])), 'wb') as f:
     f.write(res.content)

