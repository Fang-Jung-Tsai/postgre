#the "Answer to the Ultimate Question of Life, the Universe, and Everything"
import json
from datetime import datetime
from .ctao2_nsgstring import alnum_uuid
from .ctao2_hostmetadata import hostmetadata

def split_list_to_chunks(lst: list, number: int) -> list:
    '''Yield successive n-sized chunks from lst.'''
    return [ lst[i:i + number] for i in range(0, len(lst), number) ]

def numbering_list(lst: list) -> list:
    '''替每一筆資料 編上流水號'''
    return [ [num, row] for num, row in zip( range(len(lst)) , lst) ]

def google_map_api(addr:str) ->dict:
    import requests
    #google_api_key
    api_key     = f'''AIzaSyC9OvuNmrvtmNvVORtWEjo5GyLyr21yATc'''
    url_address = f'''https://maps.googleapis.com/maps/api/geocode/json?address={addr}&key={api_key}'''
    
    #讀取網址的json檔
    response = requests.get(url_address)
    #print(response.apparent_encoding)確認是否為utf-8
    response.encoding='utf-8'
    jsondata = response.text
    #抓取特定資料(抓取更深入且特定的資料並顯示出)
    jsondata=json.loads(jsondata)

    rdset  = {}  
    if jsondata['status'] == 'OK' :            
        rdset['lat'] = jsondata['results'][0]['geometry']['location']['lat']
        rdset['lng'] = jsondata['results'][0]['geometry']['location']['lng']
        rdset['formatted_address']  = jsondata['results'][0]['formatted_address']
        rdset['location_type']      = jsondata['results'][0]['geometry']['location_type']
        rdset['json'] = jsondata
    return rdset

# my Exception 
class TODEL (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TODEL(功能已經移除)'''
        return 

class TOFIX (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO FIX(有BUG)'''
        return 

class TODEV (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO DEV(發展中的功能)'''
        return
    
class answer:
    def __init__(self):
        self.tw_county_code_list    = ['10002', '10004', '10005', '10007', '10008', '10009', '10010', 
                                       '10013', '10014', '10015', '10016', '10017', '10018', '10020', 
                                       '63000', '64000', '65000', '66000', '67000', '68000', '91000', '92000']

        self.ktt_county_code_list    = ['63000', '65000', '10002']

        self.ol_county_code_list     = ['10016', '91000', '92000']

        self.uuid               = alnum_uuid()
        self.host               = hostmetadata()
        self.begtime            = datetime.now()
        self.msgtime            = [['beg', self.begtime, 0]]            

    def tick(self):
        import inspect
        curframe        = inspect.currentframe()
        calframe        = inspect.getouterframes(curframe, 2)
        caller_name     = calframe[1][3]

        time_point      = datetime.now()
        time_diff       = (time_point - self.begtime)

        seconds         = (time_diff).seconds 
        ms              = (time_diff).microseconds 

        self.msgtime.append( [ caller_name, time_point, seconds  ] )
        return '{2}_({0:03}s).({1:06}ms)\t\t'.format(seconds, ms, self.uuid)


if __name__ == '__console__' or __name__ == '__main__':
    import pandas
    from simpledbf import Dbf5
    
    srcdb_file  =  r'C:/Users/ctyang/Desktop/crying_freeman/data_nsg/_Total_err3_s2.dbf'

    dbf = Dbf5 (srcdb_file, codec='big5')
    df  = dbf.to_dataframe()

    for ind, row in df.iterrows():
        print (row['CNTY_NAME'] + row['TOWN_NAME'] + row['ATRACTNAME']  + row['ANEIGHBORH'])
    
    ans = answer()
    add = google_map_api('台灣大學')
    print ( ans.tick() )
    print ( ans.tw_county_code_list )
    print(add)