#coding=utf-8
import os, json, jsonpath, pandas
from lily.core import webapi, user_argument
from lily.aws_adapter import DynamoDB

class cwb6068(webapi):
    def __init__(self):
        ### 政府資料開放平台 顯著有感地震報告 https://data.gov.tw/dataset/6068
        url = 'https://opendata.cwb.gov.tw/api/v1/rest/datastore/E-A0015-001?Authorization=rdec-key-123-45678-011121314'        

        super().__init__(url)

        argu = user_argument()

        #save self.json to json file in directory (argu.factory)
        with open( os.path.join(argu.factory, 'cwb6068_example.json'), 'w', encoding='utf-8') as f:
            json.dump(self.json, f, ensure_ascii=False, indent=4)

        self.read_earthquake_df()

        self.read_station_df()

    def upload_to_dynamoDB(self):
        eq = self.EarthquakeDF
        st = self.StationDF

        dynamoDB_eq     = DynamoDB('data_cwb6068', 'us-east-1')
        dynamoDB_eq.save_new_item(eq)

        dynamoDB_sta    = DynamoDB('data_cwb6068_station', 'us-east-1')
        dynamoDB_sta.save_new_item(st)

    def read_earthquake_df(self):        
        #using jsonpath to get data (earthquake list) from json
        #'$..records.Earthquake.*' means get all data in records.Earthquake 
        eq_list = jsonpath.jsonpath(self.json, '$..records.Earthquake.*')

        self.EarthquakeDF = pandas.DataFrame() ### store earthquake list

        #get data whose key is in ['EarthquakeNo', 'ReportContent','ReportColor',
        #                          'Web', 'ReportImageURI','ShakemapImageURI']
        self.EarthquakeDF['EarthquakeNo']       = jsonpath.jsonpath(eq_list , f'$..EarthquakeNo')
        self.EarthquakeDF['ReportContent']      = jsonpath.jsonpath(eq_list , f'$..ReportContent')
        self.EarthquakeDF['ReportColor']        = jsonpath.jsonpath(eq_list , f'$..ReportColor')
        self.EarthquakeDF['Web']                = jsonpath.jsonpath(eq_list , f'$..Web')
        self.EarthquakeDF['ReportImageURI']     = jsonpath.jsonpath(eq_list , f'$..ReportImageURI')
        self.EarthquakeDF['ShakemapImageURI']   = jsonpath.jsonpath(eq_list , f'$..ShakemapImageURI')

        #get data f'$.records.Earthquake.*.EarthquakeInfo.Magnitude'
        self.EarthquakeDF['OriginTime']         = jsonpath.jsonpath(eq_list , f'$..EarthquakeInfo.OriginTime')        
        self.EarthquakeDF['FocalDepth']         = jsonpath.jsonpath(eq_list , f'$..EarthquakeInfo.FocalDepth')        
        self.EarthquakeDF['EpicenterLatitude']  = jsonpath.jsonpath(eq_list , f'$..EarthquakeInfo.Epicenter.EpicenterLatitude')
        self.EarthquakeDF['EpicenterLongitude'] = jsonpath.jsonpath(eq_list , f'$..EarthquakeInfo.Epicenter.EpicenterLongitude')
        self.EarthquakeDF['MagnitudeValue']     = jsonpath.jsonpath(eq_list , f'$..EarthquakeInfo.EarthquakeMagnitude.MagnitudeValue')
        self.EarthquakeDF['MagnitudeType']      = jsonpath.jsonpath(eq_list , f'$..EarthquakeInfo.EarthquakeMagnitude.MagnitudeType')

        self.EarthquakeDF.set_index('EarthquakeNo', inplace=True)

    def read_station_df(self):
        EarthquakeNo_list = jsonpath.jsonpath(self.json , f'$..records.Earthquake.*.EarthquakeNo') 
        # f'$.records.Earthquake.*.Intensity.ShakingArea'
        # means get all data in records.Earthquake.*.Intensity.ShakingArea
        SA_list = jsonpath.jsonpath(self.json, '$..records.Earthquake.*.Intensity.ShakingArea')
                 
        self.StationDF = pandas.DataFrame()     #### store station observation data
             
        for EarthquakeNo, SA in zip(EarthquakeNo_list, SA_list):
            tempDF = pandas.DataFrame()

            #select all Eqstaion in Intensity where Eqstaion.pga is exist
            #f'$..EqStation[?(@.pga)]' means get all data in EqStation whose pga is exist
            eqstation_info = jsonpath.jsonpath(SA, '$..EqStation[?(@.pga)]')

            #read attributes in eqstation_info ["StationID", "StationName", "InfoStatus", "BackAzimuth", 
            # "EpicenterDistance", "SeismicIntensity", "StationLatitude","StationLongitude,"WaveImageURI"]
            tempDF['StationID']         = jsonpath.jsonpath(eqstation_info, '$..StationID')
            tempDF['StationName']       = jsonpath.jsonpath(eqstation_info, '$..StationName')
            tempDF['InfoStatus']        = jsonpath.jsonpath(eqstation_info, '$..InfoStatus')
            tempDF['BackAzimuth']       = jsonpath.jsonpath(eqstation_info, '$..BackAzimuth')
            tempDF['EpicenterDistance'] = jsonpath.jsonpath(eqstation_info, '$..EpicenterDistance')
            tempDF['SeismicIntensity']  = jsonpath.jsonpath(eqstation_info, '$..SeismicIntensity')
            tempDF['StationLatitude']   = jsonpath.jsonpath(eqstation_info, '$..StationLatitude')
            tempDF['StationLongitude']  = jsonpath.jsonpath(eqstation_info, '$..StationLongitude')
            tempDF['WaveImageURI']      = jsonpath.jsonpath(eqstation_info, '$..WaveImageURI')

            #read attributes in eqstation_info ["pga.EWComponent", "pga.NSComponent", "pga.VComponent",
            # "pga.IntScaleValue", "pgv.EWComponent", "pgv.NSComponent", "pgv.VComponent", "pgv.IntScaleValue"]
            tempDF['pga_EWComponent']   = jsonpath.jsonpath(eqstation_info, '$..pga.EWComponent')
            tempDF['pga_NSComponent']   = jsonpath.jsonpath(eqstation_info, '$..pga.NSComponent')
            tempDF['pga_VComponent']    = jsonpath.jsonpath(eqstation_info, '$..pga.VComponent')
            tempDF['pga_IntScaleValue'] = jsonpath.jsonpath(eqstation_info, '$..pga.IntScaleValue')
            tempDF['pgv_EWComponent']   = jsonpath.jsonpath(eqstation_info, '$..pgv.EWComponent') 
            tempDF['pgv_NSComponent']   = jsonpath.jsonpath(eqstation_info, '$..pgv.NSComponent')
            tempDF['pgv_VComponent']    = jsonpath.jsonpath(eqstation_info, '$..pgv.VComponent')
            tempDF['pgv_IntScaleValue'] = jsonpath.jsonpath(eqstation_info, '$..pgv.IntScaleValue')

            #add EarthquakeNo column
            tempDF['EarthquakeNo'] = EarthquakeNo

            #add EarthquakeNo_StationID column
            tempDF['EarthquakeNo_StationID'] = tempDF['EarthquakeNo'].astype(str) + '_' + tempDF['StationID'].astype(str)
            
            #append tempDF to self.StationDF
            self.StationDF = pandas.concat([self.StationDF, tempDF], ignore_index=True)

        self.StationDF.set_index('EarthquakeNo_StationID', inplace=True)

if __name__ =='__main__':
    cwbobj = cwb6068()
    cwbobj.upload_to_dynamoDB()

