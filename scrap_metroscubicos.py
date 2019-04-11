# -*- coding: utf-8 -*-
"""
Created on Thu Jan 17 00:01:44 2019

@author: javie
"""

import requests
from pandas.io.json import json_normalize
import pandas as pd
import math
import time

from pymongo import MongoClient

def get_attribute(name):
    try:
        return list(filter(lambda x : x['id'] == name, attributes))[0].get('value_name')
    except:
        return ""

#https://api.mercadolibre.com/classified_locations/countries/MX --> devuelve los estados de un country
#https://api.mercadolibre.com/classified_locations/states/TUxNUERJUzYwOTQ --> devuelve las ciudades de un estado
#https://api.mercadolibre.com/classified_locations/cities/TUxNQ0JFTjM2MjQ --> devuelve neighborhoods de una ciudad 
#https://api.mercadolibre.com/classified_locations/neighborhoods/TUxNQk7BUDUwNzQ <-- devuelve sub neighborhoods si existieran
#https://api.mercadolibre.com/sites/MLM/categories <-- devuelve todas la categorias para MLM (mercado libre mexico)
#https://api.mercadolibre.com/categories/MLM1459 <-- retorna sub categorias de MLM1459
 
#https://api.mercadolibre.com/sites/MLM/categories

    
#r = requests.get('https://api.mercadolibre.com/sites/MLM/search?category=MLM1459')
conn = MongoClient()
db = conn.metroscubicos
collection = db.propiedades 

r = requests.get('https://api.mercadolibre.com/sites/MLM/search?category=MLM1472&state=TUxNUERJUzYwOTQ&city=TUxNQ0JFTjM2MjQ') # casas
results = []
code_pais = "MX"
categorias = requests.get("https://api.mercadolibre.com/categories/MLM1459").json().get('children_categories')

categorias = ['MLM1479','MLM1480','MLM1467','MLM1468']
#DEPARTAMENTO
#MLM1479 # renta
#MLM1480 # Compra
# CASA
#MLM1467 # renta
#MLM1468 # compra


estados = requests.get("https://api.mercadolibre.com/classified_locations/countries/" + code_pais).json().get('states')

nombre_estado = "Distrito Federal"
id_estado = "TUxNUERJUzYwOTQ"

id_ciudad = "TUxNQ0NVQTczMTI"
nombre_ciudad = "Cuauhtémoc"

#print(nombre_estado)
#ciudades = requests.get("https://api.mercadolibre.com/classified_locations/states/" + id_estado ).json().get('cities')
#for ciudad in ciudades:
    #nombre_ciudad = ciudad['name']
    #id_ciudad = ciudad['id']
    #neighborhoods = requests.get("https://api.mercadolibre.com/classified_locations/cities/" + id_ciudad).json().get('neighborhoods')
neighborhoods =  {  "id": "TUxNQkNPTjI4MDYzNQ",
     "name": "Condesa"
},{
     "id": "TUxNQkhJUDg2MzE",
     "name": "Hipódromo De La Condesa"
},{
     "id": "TUxNQkhJUDg0OTk",
     "name": "Hipódromo"
}
for neighborhood in neighborhoods:
    nombre_neighborhood = neighborhood['name']
    id_neighborhood = neighborhood['id']
    for categoria in categorias:
        #nombre_categoria = categoria['name']
        id_categoria = categoria
        contador = 0
        offset = 50
        while(True):
            pagina = offset* contador
            limit = pagina + 49
            if(pagina>1000):
                break                
            #print(nombre_estado," --> ",nombre_ciudad," --> ",nombre_neighborhood," --> ",nombre_categoria ," -->", pagina ," a ", limit)
            url = 'https://api.mercadolibre.com/sites/MLM/search?category='+ id_categoria +'&state='+ id_estado +'&city=' + id_ciudad + '&neighborhood=' + id_neighborhood + '&offset='+str(pagina)
            print(url)
            r = requests.get(url)
            data = r.json()
            paging = data.get('paging')
            total_resultados = paging['total']
            results = data.get('results')
            secondary_results = data.get('secondary_results')
            paginas = math.floor(total_resultados/offset)
            if len(results) >0:
                collection.insert_many(results)
            if len(secondary_results) >0:
                collection.insert_many(secondary_results) 
            if(contador == paginas):
                break
            contador += 1
    time.sleep(30)

 
