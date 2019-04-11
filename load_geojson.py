# -*- coding: utf-8 -*-
"""
Created on Sat Jan 26 23:36:07 2019

@author: javier
"""

# Importacion de archivos x
import json
import pprint
from pymongo import MongoClient, GEO2D

conn = MongoClient()
db = conn.metroscubicos
propiedades = db.propiedades
col_metro = db.metro
col_cdmx = db.cdmx
col_ageb = db.ageb
pgj = db.pgj


## CARGAR ARCHIVOS GEOJSON EN COLECCION MONGO
data_metro = open("data/estaciones-metro.geojson")
x_metro = json.load(data_metro)

data_cdmx = open("data/cdmx.geojson")
x_cdmx = json.load(data_cdmx)

data_ageb = open("data/ageb.geojson",encoding="utf8")
x_ageb = json.load(data_ageb)

#data_pgj = open("data/carpetas-de-investigacion-pgj-cdmx.json",encoding="utf8")
#x_pgj = json.load(data_pgj)

variable = GEO2D
#col_metro.create_index([("location", GEO2D)])
col_metro.create_index([("geometry", "2dsphere")])
col_cdmx.create_index([("geometry", "2dsphere")])
col_ageb.create_index([("geometry", "2dsphere")])
#pgj.create_index([("geometry", "2dsphere")])
#propiedades.create_index([("geometry", "2dsphere")])

#for i in x["features"]:
#    pprint.pprint(i)
#    col.insert_one(i)
col_metro.insert_many(x_metro["features"])
col_cdmx.insert_many(x_cdmx["features"])
col_ageb.insert_many(x_ageb["features"])
#pgj.insert_many(x_pgj)


LON, LAT = 19.385422, -99.176435
RADIUS = 1

one = col_metro.find_one()
pprint.pprint(one)
#for cerca in col.find({"geometry": {"$near": [LON,LAT]}}):
#    pprint.pprint(cerca) 

DIST_KMS = 1.2
DIST_MILLAS = 1
dos = col_metro.find({"geometry": { "$geoWithin":{ "$centerSphere": [ [LAT, LON ], (DIST_KMS * 0.621371)/ 3963.2  ] } } })
for j in dos:
    pprint.pprint(j)
    x_lat = j.get('geometry').get('coordinates')[0]
    y_lon = j.get('geometry').get('coordinates')[1]
    print(x_lat,y_lon)

tres = col_metro.find({ "geometry": { "$nearSphere": { "$geometry": { "type": "Point", "coordinates": [ LAT, LON ] }, "$maxDistance": 1000 * DIST_KMS } } })
for j in tres:
    pprint.pprint(j)

colonia = col_cdmx.find_one({ "geometry": { "$geoIntersects": { "$geometry": { "type": "Point", "coordinates": [ LAT,LON] } } } })    
ageb = col_ageb.find_one({ "geometry": { "$geoIntersects": { "$geometry": { "type": "Point", "coordinates": [ LAT,LON] } } } })    
    
cercanos = col_metro.aggregate([
   {
     "$geoNear": {
        "near": { "type": "Point", "coordinates": [ LAT , LON ] },
        "distanceField": "dist.calculated",
        "maxDistance": 1100,
        "includeLocs": "dist.location",
        "num": 5,
        "spherical": True
     }
   }
])
    
for cercano in cercanos:
    print(cercano.get('dist').get('calculated'))
    print(cercano.get('properties').get('stop_name'))


###################################

# =============================================================================
# db = MongoClient().geo_example
# db.places.create_index([("loc", GEO2D)])
# result = db.places.insert_many([{"loc": [2, 5]},{"loc": [30, 5]},{"loc": [1, 2]},{"loc": [4, 4]}])
# 
# for doc in db.places.find({"loc": {"$near": [3, 6]}}).limit(3):
#     pprint.pprint(doc) 
# =============================================================================
