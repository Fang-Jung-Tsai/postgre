#coding=utf-8
import io
import os
import re
import pandas
import matplotlib.pyplot as plt
import jsonpath
from webapi import webapi

class cwa_d0047( webapi ):
   
    @staticmethod
    def cwa_weather_description (value):
        data = []
        # Split the value by '。' and remove the last element
        value = value.split('。')
        value.pop()

        # Extract rainfall_probability from the 2nd element of value
        rainfall_probability = int(re.search(r'\d+', value[1]).group())

        # Extract temperature from the 3rd element of value
        temperature = int(re.search(r'\d+', value[2]).group())

        # Extract relative_humidity from the 6th element of value
        relative_humidity = int(re.search(r'\d+', value[5]).group())

        # Create a dictionary to store rainfall_probability, temperature, relative_humidity, weather, comfort, wind
        value_dict = {'rainfall_probability': rainfall_probability, 'temperature': temperature,
                      'relative_humidity': relative_humidity, 'weather': value[0], 'comfort': value[3], 'wind': value[4]}
     
        return value_dict

    def __init__(self, county_code:int=63):
        # Government Open Data Platform
        userkey     = 'CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE'
        urlprefix   = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/'
        dataid      = f'F-D0047-{county_code:03d}'
        url         = f'''{urlprefix}{dataid}?Authorization={userkey}&format=JSON&sort=time'''
        
        super().__init__(url)

        #a empty dict to store the data for each county

        self.datadict = {}
        #self.json is the json data from the url and get from the webapi class
        self.datadict['locationName']           = jsonpath.jsonpath (self.json, '$..locationName')        
        self.datadict['endTime']                = jsonpath.jsonpath (self.json, '$..weatherElement[?(@.elementName=="WeatherDescription")].time[0].endTime')
        self.datadict['startTime']              = jsonpath.jsonpath (self.json, '$..weatherElement[?(@.elementName=="WeatherDescription")].time[0].startTime')
        self.datadict['WeatherDescription']     = jsonpath.jsonpath (self.json, '$..weatherElement[?(@.elementName=="WeatherDescription")].time[0].elementValue[0].value')
 
        if len(set(self.datadict['startTime'])) != 1 or len(set(self.datadict['endTime'])) != 1:
            raise ValueError('time_beg or time_end is not 1')
            
        #a empty list to store the data for each town    
        self.datarows = []
        for location, elem, begtime, endtime in zip(self.datadict['locationName'], self.datadict['WeatherDescription'], self.datadict['startTime'], self.datadict['endTime']):
            decomposition =  self.cwa_weather_description(elem)
            decomposition['locationName'] = location
            decomposition['startTime'] =    begtime
            decomposition['endTime'] =      endtime 
            self.datarows.append( decomposition  )

        #create a dataframe from the datarows
        self.dataframe = pandas.DataFrame(self.datarows, columns=['locationName', 'startTime', 'endTime', 'rainfall_probability', 'temperature', 'relative_humidity', 'weather', 'comfort', 'wind' ])
        
        #cast types of columns: locationName, rainfall_probability, temperature, relative_humidity, weather, comfort, wind
        #cast type of Time to pandas.timestamp and timezone to Asia/Taipei
        self.dataframe['startTime'] = pandas.to_datetime(self.dataframe['startTime'])
        self.dataframe['endTime']   = pandas.to_datetime(self.dataframe['endTime'])
        self.dataframe['startTime'] = self.dataframe['startTime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')
        self.dataframe['endTime']   = self.dataframe['endTime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')

        #cast [rainfall_probability, temperature, relative_humidity] to int16
        self.dataframe['rainfall_probability'] = self.dataframe['rainfall_probability'].astype('int16')
        self.dataframe['relative_humidity'] = self.dataframe['relative_humidity'].astype('int16')
        self.dataframe['temperature'] = self.dataframe['temperature'].astype('int16')        

        #add county_code to dataframe
        self.dataframe['county_code']  = county_code
            
class cwa_towns():

    def __init__(self):
        
        #a empty dict to store the data for each county
        self.datadict = {}
        self.datarows = []
        self.dataframe = None
        #county_code from 1 to 85 , step =  4
        for county_code in range(1, 86, 4):
            try:
                cwa_obj = cwa_d0047(county_code)
                self.datadict[county_code] = cwa_obj.datadict
                self.datarows.extend(cwa_obj.datarows)  
                print (f'county_code {county_code} is available'   )
            except ValueError:
                print(f'county_code {county_code} is not available')
                continue
            self.dataframe = pandas.concat([self.dataframe, cwa_obj.dataframe], ignore_index=True)

if __name__ =='__main__':
    cwa = cwa_towns()

    import postgis_CE13058
    try:
        pg_obj = postgis_CE13058.postgis_CE13058()

        pg_obj.write_data(cwa.dataframe, 'data_rosa_ctyang_cwa_d0047')   
        
    except Exception as e:
        print (e)