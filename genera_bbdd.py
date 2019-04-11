# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 23:36:07 2019

@author: javier
"""

# Importacion de archivos x
import json
import html
import pprint
import pandas as pd
from pymongo import MongoClient, GEO2D

conn = MongoClient()
db = conn.metroscubicos
col_metro = db.metro
col_cdmx = db.cdmx
col_ageb = db.ageb
propiedades = db.propiedades
 
LAT, LON = 19.406402, -99.178789
RADIUS = 1
 
one = col_metro.find_one()
pprint.pprint(one)

DIST_KMS = 1.2
DIST_MILLAS = 1

def get_attribute(name,attributes):
    try:
        return list(filter(lambda x : x['id'] == name, attributes))[0].get('value_name')
    except:
        return ""


def get_ageb(colection, latitude,longitude):
    try:
        ageb = colection.find_one({ "geometry": { "$geoIntersects": { "$geometry": { "type": "Point", "coordinates": [ longitude , latitude ] } } } })    
        return ageb.get('properties').get('CVEGEO')
    except:
        return ""


def cercanos(colection, latitude,longitude, max_distance):    
    cercanos = colection.aggregate([
       {
         "$geoNear": {
            "near": { "type": "Point", "coordinates": [ longitude , latitude ] },
            "distanceField": "dist.calculated",
            "maxDistance": max_distance,
            "includeLocs": "dist.location",
            "num": 5,
            "spherical": True
         }
       }
    ])
    return cercanos
    
   

    
results = []
df = pd.DataFrame(columns=['TITLE','PRICE','CURRENCY_ID','CONDITION','AVAILABLE_QUANTITY','ADDRESS','NEIGHBORHOOD','LAT','LON','HAS_TELEPHONE_LINE','BEDROOMS','COVERED_AREA','FULL_BATHROOMS','TOTAL_AREA','OPERATION','PROPERTY_TYPE','ITEM_CONDITION','AGEB','METRO_500MTS'])  
for propiedad in propiedades.find():
    title = propiedad['title']
    price = propiedad['price']
    currency_id = propiedad['currency_id']
    condition = propiedad['condition']
    
    available_quantity = propiedad['available_quantity']
    
    #locacion
    location = propiedad['location']
    address = location['address_line']
    neighborhood = location['neighborhood']
    neighborhood_name = neighborhood['name']
    
    latitud = location['latitude']
    longitud = location['longitude']
    
    attributes = propiedad['attributes']

    if latitud and longitud:
        metros_cercanos_500mts = cercanos(col_metro, latitud,longitud, 500)
        metros_cercanos_count = len(list(metros_cercanos_500mts))
        ageb = get_ageb(col_ageb, latitud,longitud)
        print(metros_cercanos_count, ageb)
        df = df.append({'TITLE':html.unescape(title), 
                        'PRICE':price,
                        'CURRENCY_ID':currency_id,
                        'CONDITION':condition,
                        'AVAILABLE_QUANTITY':available_quantity,
                        'ADDRESS':address,
                        'NEIGHBORHOOD':neighborhood_name,
                        'LAT':latitud,
                        'LON':longitud,
                        'HAS_TELEPHONE_LINE':get_attribute('HAS_TELEPHONE_LINE',attributes),
                        'BEDROOMS':get_attribute('BEDROOMS',attributes),
                        'COVERED_AREA':get_attribute('COVERED_AREA',attributes),
                        'FULL_BATHROOMS':get_attribute('FULL_BATHROOMS',attributes),
                        'TOTAL_AREA':get_attribute('TOTAL_AREA',attributes),
                        'OPERATION':get_attribute('OPERATION',attributes),
                        'PROPERTY_TYPE':get_attribute('PROPERTY_TYPE',attributes),
                        'ITEM_CONDITION':get_attribute('ITEM_CONDITION',attributes),
                        'AGEB': ageb,
                        'METRO_500MTS':metros_cercanos_count
                        }, ignore_index=True)


###################################

# =============================================================================
# db = MongoClient().geo_example
# db.places.create_index([("loc", GEO2D)])
# result = db.places.insert_many([{"loc": [2, 5]},{"loc": [30, 5]},{"loc": [1, 2]},{"loc": [4, 4]}])
# 
# for doc in db.places.find({"loc": {"$near": [3, 6]}}).limit(3):
#     pprint.pprint(doc) 
# =============================================================================

#df['AGEB'] = df['AGEB']

print(df['AGEB'])
df2 = pd.read_excel("rezago_social.xlsx",converters={'folio_ageb':str,'cve_ent':int})
df2['folio_ageb'] = df2['folio_ageb'].apply(lambda x: x.zfill(13))
muestra = df2.head()    
  
df_join = pd.merge(df, df2, left_on='AGEB',right_on='folio_ageb',how='left',suffixes=('_left','_right'))
    
df_join.to_csv('metroscubicos_cdmx_join.csv', encoding='utf-8')
df.to_csv('metroscubicos_cdmx.csv', encoding='utf-8')