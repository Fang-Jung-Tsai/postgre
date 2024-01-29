#coding=utf-8
import pandas
import jsonpath
from webapi import webapi

class cwa_a0015_eq( webapi ):
   
    def __init__(self):

        userkey     = 'CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE'
        urlprefix   = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/'
        dataid      = 'E-A0015-001'
        #url = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/EA0015-001/?Authorization=CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE&format=JSON&sort=time'
        url         = f'''{urlprefix}{dataid}?Authorization={userkey}&format=JSON&sort=time'''

        ### store earthquake list
        self.datadict = {} 

        #get data from url
        super().__init__(url)

        #get data whose key is in ['records.Earthquake']
        eq_list = jsonpath.jsonpath(self.json, '$..records.Earthquake.*')

        element_list = ['EarthquakeNo', 'ReportContent','ReportColor', 'Web', 'ReportImageURI','ShakemapImageURI',
                        'EarthquakeInfo.OriginTime', 'EarthquakeInfo.FocalDepth', 
                        'EarthquakeInfo.Epicenter.EpicenterLatitude', 'EarthquakeInfo.Epicenter.EpicenterLongitude', 
                        'EarthquakeInfo.EarthquakeMagnitude.MagnitudeValue', 'EarthquakeInfo.EarthquakeMagnitude.MagnitudeType']

        for element in element_list:
            #shortname for element
            short_element_name = element.split('.')[-1]
            self.datadict[short_element_name] = jsonpath.jsonpath(eq_list , f'$..{element}')

        #convert self.datadict to dataframe
        self.dataframe = pandas.DataFrame(self.datadict)

        #convert column OriginalTime to timestamp and timezone to Asia/Taipei
        self.dataframe['OriginTime'] = pandas.to_datetime(self.dataframe['OriginTime'])
        self.dataframe['OriginTime'] = self.dataframe['OriginTime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')

        ##############################################################
        #SA_list = jsonpath.jsonpath(self.json, '$..records.Earthquake.*.Intensity.ShakingArea')
        self.datadict['ShakingArea'] = jsonpath.jsonpath(eq_list, '$..ShakingArea')

        self.StationDF = pandas.DataFrame()     #### store station observation data

        for EarthquakeNo, SA in zip(self.datadict["EarthquakeNo"], self.datadict["ShakingArea"]):
            tempDF = pandas.DataFrame()

            #select all Eqstaion in Intensity where Eqstaion.pga is exist
            #f'$..EqStation[?(@.pga)]' means get all data in EqStation whose pga is exist
            eqstation_info = jsonpath.jsonpath(SA, '$..EqStation[?(@.pga)]')

            #read attributes in eqstation_info
            att_list = ["StationID", "StationName", "InfoStatus", "BackAzimuth","EpicenterDistance", 
                        "SeismicIntensity", "StationLatitude", "StationLongitude", "WaveImageURI"]
            for att in att_list:
                tempDF[att] = jsonpath.jsonpath(eqstation_info, f'$..{att}')

            #read more attributes in eqstation_info
            att_list2 = ["pga.EWComponent", "pga.NSComponent", "pga.VComponent","pga.IntScaleValue",
                         "pgv.EWComponent", "pgv.NSComponent", "pgv.VComponent", "pgv.IntScaleValue"]
            for att in att_list2:
                #shortname for element replace '.' to '_'
                short_element_name = att.replace('.', '_')
                tempDF[short_element_name] = jsonpath.jsonpath(eqstation_info, f'$..{att}')

            #add EarthquakeNo column
            tempDF['EarthquakeNo'] = EarthquakeNo

            #add EarthquakeNo_StationID column
            tempDF['EarthquakeNo_StationID'] = tempDF['EarthquakeNo'].astype(str) + '_' + tempDF['StationID'].astype(str)
            
            #append tempDF to self.StationDF
            self.StationDF = pandas.concat([self.StationDF, tempDF], ignore_index=True)
    
if __name__ =='__main__':
    import postgis_CE13058
    postgis_obj = postgis_CE13058.postgis_CE13058()
    #get new data from web
    cwa_obj = cwa_a0015_eq()

    # initailize database at the first time
    # postgis_obj.drop_table('data_rosa_ctyang_cwa_a0015_eq')
    # postgis_obj.drop_table('data_rosa_ctyang_cwa_a0015_st')
    # postgis_obj.write_data(cwa_obj.dataframe, 'data_rosa_ctyang_cwa_a0015_eq')   
    # postgis_obj.write_data(cwa_obj.StationDF, 'data_rosa_ctyang_cwa_a0015_st')

    old_eq = postgis_obj.read_data('data_rosa_ctyang_cwa_a0015_eq') #get old data in database
    old_st = postgis_obj.read_data('data_rosa_ctyang_cwa_a0015_st') #get old st data in database

    #new data on web but not in database
    new_eq = cwa_obj.dataframe[~cwa_obj.dataframe['EarthquakeNo'].isin(old_eq['EarthquakeNo'])]   

    #new st data on web but not in database
    new_st = cwa_obj.StationDF[~cwa_obj.StationDF['EarthquakeNo_StationID'].isin(old_st['EarthquakeNo_StationID'])]

    postgis_obj.append_data(new_eq, 'data_rosa_ctyang_cwa_a0015_eq')
    postgis_obj.append_data(new_st, 'data_rosa_ctyang_cwa_a0015_st')