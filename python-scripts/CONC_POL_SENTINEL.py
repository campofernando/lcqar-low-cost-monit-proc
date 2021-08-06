# -*- coding: utf-8 -*-


import datetime
from datetime import date, timedelta
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import ee
from termcolor import colored
import shapely
from calendar import monthrange

# --------- Initialize the GEE
#ee.Authenticate()
ee.Initialize()

#%% -------------- Upload área de estudo (.shp)
dados = pd.read_csv('local_ratao.csv')
dados = dados.drop_duplicates(['Latitude', 'Longitude'])

dados = gpd.GeoDataFrame(dados, geometry=gpd.points_from_xy(dados.Longitude, dados.Latitude))
dados = dados.reset_index(drop = True)
dados['id'] = range(1, len(dados)+1)

#%% -------------- Função GEE Sentinel 5-p
 
def sentinelDaily(collecPol, columnPol, y, m, d):
    
    imagesMonth = []
    
    dadosMensais = pd.DataFrame()
 
    # import the Gee products
    collection = ee.ImageCollection(collecPol)
        
    # Set date in ee date format
    startdate = ee.Date.fromYMD(y,1,1)
    enddate = ee.Date.fromYMD(y+1,12,31)
     
    # Filter chirps
    Pchirps = collection.filterDate(startdate, enddate).sort('system:time_start', False)\
              .select(columnPol)
                
    for idx in range(0,len(dados)):
        
        print(colored('Ponto '+ str(dados['id'][idx]) +' de '+str(len(dados)), 'red'))
                                    
        feature = ee.Feature(ee.Geometry.Point([dados.geometry[idx].x, dados.geometry[idx].y]))
                
        area = ee.FeatureCollection(feature)
        
        # calculate the daily mean    
        def calcMonthlyMean(imageCollection):
            
            mylist = ee.List([])
            
            w = imageCollection.filter(ee.Filter.calendarRange(y, y, 'year'))\
                .filter(ee.Filter.calendarRange(m, m, 'month'))\
                .filter(ee.Filter.calendarRange(d, d, 'DAY_OF_MONTH')).mean()
                               
                                                            
            mylist = mylist.add(w.set('year', y).set('month', m).set('DAY_OF_YEAR', d)\
                                .set('date', ee.Date.fromYMD(y,m,d))\
                                .set('system:time_start',ee.Date.fromYMD(y,m,d)))
                                                          
            return ee.ImageCollection.fromImages(mylist)
            

        # run the calcMonthlyMean function
        monthlyChirps= ee.ImageCollection(calcMonthlyMean(Pchirps))
    
        # select the region of interest, 25000 is the cellsize in meters
        monthlyChirps = monthlyChirps.getRegion(area, 1,"epsg:4326").getInfo()
                     
        # get date
        monthData = pd.DataFrame(monthlyChirps, columns = monthlyChirps[0])
        
        # remove the first line
        monthData = monthData[1:]
        monthData[columnPol] = monthData[columnPol].astype(float)
        monthData['id'] = monthData['id'].astype(int)
        monthData = monthData.groupby(['id'], as_index = False)[columnPol].mean().sort_values(by = ['id'])
        monthData['month'] = m
        monthData['id'] = d
        monthData['year'] = y
        monthData = monthData.rename(columns = {'id': 'days'})
        monthData['Ponto'] = 'Ponto '+ str(dados['id'][idx])
        monthData['lon'] = dados.geometry[idx].x
        monthData['lat'] = dados.geometry[idx].y
        monthData['local'] = dados.Local[idx]         
                                 
        dadosMensais = pd.concat([dadosMensais, monthData]) 
                                                                                                                               
    return dadosMensais, imagesMonth

#%% -------------- dados GEE Sentinel 5-p

#ano, mes, dia
start_date = date(2018, 6, 28)
end_date = date(2018, 7, 5)
delta = timedelta(days=1)

outputFile = open("DADOS_RATAO.csv", "w+")
cabecalho=["Data"]
for i in range (0,len(dados)):
    cabecalho+=[dados.Local[i]]
    
outputFile.write(','.join(cabecalho) + "\n")
current_data = start_date

while current_data <= end_date:
    NO2Mensais19, imagesMonth = sentinelDaily('COPERNICUS/S5P/OFFL/L3_NO2', 'NO2_column_number_density', 
                                              current_data.year, 
                                              current_data.month,
                                              current_data.day)
    dataStr = current_data.strftime("%Y-%m-%d")
    colunas = [dataStr]
    
    for i in range (0,len(dados)):
        
        
       
        resultado = str(NO2Mensais19.NO2_column_number_density.to_list()[i])
        colunas +=[resultado]
        
    outputFile.write(','.join(colunas) + "\n")
    
    current_data += delta

outputFile.close()


#%% -------------- formatando dados GEE Sentinel 5-p
NO2Mensais19 = gpd.GeoDataFrame(NO2Mensais19, geometry = gpd.points_from_xy(NO2Mensais19.lon, NO2Mensais19.lat))
NO2Mensais19 = NO2Mensais19.reset_index(drop = True)
NO2Mensais19.plot(column = 'NO2_column_number_density')
plt.show()