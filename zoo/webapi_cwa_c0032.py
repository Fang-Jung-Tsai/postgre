#coding=utf-8
import io
import os
import re
import pandas
import requests
import datetime
import geopandas
import matplotlib.pyplot as plt
import jsonpath
from webapi import webapi

class cwa_c0032( webapi ):
   
    def __init__(self):

        # Government Open Data Platform
        userkey     = 'CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE'
        urlprefix   = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/'
        dataid      = 'F-C0032-001'
        
        #url = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001?Authorization=CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE&format=JSON&sort=time'
        url         = f'''{urlprefix}{dataid}?Authorization={userkey}&format=JSON&sort=time'''

        super().__init__(url)
    
        self.datadict   = {}
        #search all locationName element in json
        self.datadict['Cnty'] = jsonpath.jsonpath(self.json, '$..locationName')
    
        #search element named Wx
        #get its first time_element
        #then get  startTime and endTime of time_element 
        self.datadict['startTime'] = jsonpath.jsonpath(self.json, f'$..weatherElement[?(@.elementName=="Wx")].time[0].startTime')
        self.datadict['endTime']   = jsonpath.jsonpath(self.json, f'$..weatherElement[?(@.elementName=="Wx")].time[0].endTime')

        #elemComments  = ['天氣現象', '降雨機率', '舒適度', '最低溫度', '最高溫度']
        #elemEngNames  = ['WeatherDescription', 'rainfall_probability', 'comfort', 'MinT', 'MaxT']   
        elemkeywords  = ['Wx', 'PoP', 'CI', 'MinT', 'MaxT']
        for elem in elemkeywords:
            self.datadict[elem] = jsonpath.jsonpath(self.json, f'$..weatherElement[?(@.elementName=="{elem}")].time[0].parameter.parameterName')    
        
        #convert self.datadict to self.dataframe
        self.dataframe = pandas.DataFrame(self.datadict, columns=['Cnty', 'startTime', 'endTime', 'Wx', 'PoP', 'CI', 'MinT', 'MaxT'])
        #cast type of Time to pandas.timestamp and timezone to Asia/Taipei
        self.dataframe['startTime'] = pandas.to_datetime(self.dataframe['startTime'])
        self.dataframe['endTime']   = pandas.to_datetime(self.dataframe['endTime'])
        self.dataframe['startTime'] = self.dataframe['startTime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')
        self.dataframe['endTime']   = self.dataframe['endTime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')

        #cast [MinT, MaxT, Pop] to int16
        self.dataframe['MinT'] = self.dataframe['MinT'].astype('int16')
        self.dataframe['MaxT'] = self.dataframe['MaxT'].astype('int16')
        self.dataframe['PoP']  = self.dataframe['PoP'].astype('int16')

if __name__ =='__main__':
    import postgis_CE13058

    cwa = cwa_c0032()

    try:
        pg_obj = postgis_CE13058.postgis_CE13058()

        pg_obj.write_data(cwa.dataframe, 'data_rosa_ctyang_cwa_c0032')   
        
    except Exception as e:
        print (e)