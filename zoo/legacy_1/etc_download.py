import os
import re
import time
import random
import tarfile
import datetime
import requests
import json
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from dateutil.relativedelta import relativedelta
from abc import ABC, abstractmethod
import gzip

class Tisvcloud(ABC):
    @abstractmethod
    def __init__(self, month_str:str, name:str):
        
        # check the re.pattern of self.name (M03A, M04A, M05A)
        if not re.match(r'^M0[3-5]A$', name):
            raise Exception('self.name is not valid')

        self.name = name
        #set download_dir to the Downloads directory of current user's home 
        download_dir = os.path.join(os.path.expanduser('~'), 'Downloads')

        #set actual_download_dir and create it if it does not exist 
        self.actual_download_dir = os.path.join(download_dir, self.name)
        if not os.path.exists(self.actual_download_dir):
            os.makedirs(self.actual_download_dir)

        # check the format of month_str (YYYY-MM)
        if not re.match(r'^\d{4}-\d{2}$', month_str):
            raise Exception('month_str is not valid')

        self.date = datetime.datetime.strptime(month_str, "%Y-%m")      #
        self.day_beg = self.date.replace(day=1)                         # the first day of the month
        self.day_end = self.day_beg + relativedelta(months=1, days=-1)  # the last day of the month

        # create file_list for storing urls, file_names, and status of urls, downloads, and extractions
        self.file_list = {}
        for day_cur in pd.date_range(self.day_beg, self.day_end):            
            day_cur_str = day_cur.strftime("%Y%m%d") 
            file_name   = f"{self.name}_{day_cur_str}.tar.gz"
            url = f"https://tisvcloud.freeway.gov.tw/history/TDCS/{self.name}/{file_name}"

            self.file_list[day_cur_str] = {}
            self.file_list[day_cur_str]['url'] = url
            self.file_list[day_cur_str]['file_name'] = file_name
            self.file_list[day_cur_str]['status_url']      = False
            self.file_list[day_cur_str]['status_download'] = False
            self.file_list[day_cur_str]['status_extract']  = False
            self.file_list[day_cur_str]['csv_step'] = {}

            #   Attention: The file path should be noticed!
            #              Because the decompressed file will be placed in download_dir/M04A/date/hour/file.csv, 
            #              the path should be combined with os.path.join().
            #              For example: download_dir = '/home/user/Downloads', date = 20190101, hour = 00, file = TDCS_M04A_20190101_000000.csv
            #              and self.name = M04A
            #              then the file path will be /home/user/Downloads/M04A/20190101/00/TDCS_M04A_20190101_000000.csv
            #              csv_file_path = os.path.join(download_dir, self.name, day_cur_str, hour_str, f"TDCS_{self.name}_{day_cur_str}_{hour_str}{minutes_str}00.csv")      
            step_delta = datetime.timedelta(minutes=5)
            step_beg   = datetime.datetime.combine(day_cur, datetime.time(0, 0, 0))
            step_end   = datetime.datetime.combine(day_cur, datetime.time(23, 55, 0))
            for step in pd.date_range(start=step_beg, end=step_end, freq=step_delta):
                hour_str = step.strftime("%H")
                minutes_str = step.strftime("%M")
                csv_file_name = f"TDCS_{self.name}_{day_cur_str}_{hour_str}{minutes_str}00.csv"
                csv_file_path = f"{self.name}/{day_cur_str}/{hour_str}/{csv_file_name}"
                self.file_list[day_cur_str]['csv_step'][csv_file_path] = False

        self.write_file_list_json()

    def write_file_list_json(self):
        #write file_list to a json file
        self.file_list_json = os.path.join(self.actual_download_dir, f'''{self.name}_{self.date.strftime("%Y%m")}.json''')
        content = json.dumps(self.file_list, indent=4)
        with open(self.file_list_json, 'w') as f:
            f.write(content)

    def check_files(self):
        #check all urls in the file_list
        for day_cur_str in self.file_list:
            url = self.file_list[day_cur_str]['url']            
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    self.file_list[day_cur_str]['status_url'] = True
            except requests.exceptions.RequestException as err:
                self.file_list[day_cur_str]['status_url'] = False
            
            #sleep for a random time between 0 and 2 seconds for avoiding the server to block the IP    
            time.sleep(0.001 * random.randint(1, 2000)  )

        #check if all urls are valid
        self.url_status = all([self.file_list[day_cur_str]['status_url'] for day_cur_str in self.file_list])

        self.write_file_list_json()

    def download_files(self):
        #download all files from the url
        for day_str in self.file_list:
            url= self.file_list[day_str]['url']

            file_path = os.path.join(self.actual_download_dir, self.file_list[day_str]['file_name'])

            #download the file
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    with open(file_path, 'wb') as f:
                        f.write(r.content)
                    self.file_list[day_str]['status_download'] = True
                    print(f"Downloaded {file_path}")

            except requests.exceptions.RequestException as err:
                self.file_list[day_str]['status_download'] = False

            #extract the file
            try:
                with tarfile.open(file_path, 'r:gz') as tar:
                    tar.extractall(path=self.actual_download_dir)
                self.file_list[day_str]['status_extract'] = True
                print(f"Extracted {file_path}")
            except tarfile.ReadError as err:
                self.file_list[day_str]['status_extract'] = False

        #check if all files are downloaded and extracted
        self.status_exact       = all([self.file_list[day_str]['status_extract'] for day_str in self.file_list])    
        self.status_download    = all([self.file_list[day_str]['status_download'] for day_str in self.file_list])
        
        self.write_file_list_json()


    def gather_file(self):
        #threads for combining csv files
        import threading 
        threads = []

        for day_str in self.file_list:   
            #check if all csv files are downloaded and extracted
            #and gather all csv files to a single bytes list
            csv_list = []
            for csv_file_path in self.file_list[day_str]['csv_step']:
                csv_file = os.path.join(self.actual_download_dir, csv_file_path)
                if os.path.exists(csv_file):
                    self.file_list[day_str]['csv_step'][csv_file_path] = True
                    #read csv file to bytes
                    with open(csv_file, 'rb') as f:
                        csv_bytes = f.read()
                        csv_list.append(csv_bytes)
                else:
                    self.file_list[day_str]['csv_step'][csv_file_path] = False

            # gather csv bytes in csv_list to a single gz file
            # assgin the task to a independent thread
            if all(self.file_list[day_str]['csv_step'].values()):
                csv_gz = os.path.join(self.actual_download_dir, f"{self.name}_{day_str}.csv.gz")

                t = threading.Thread(target=self.combine_csv_gz, args=(csv_gz, csv_list))
                t.start()
                threads.append(t)

        self.write_file_list_json()
        for t in threads:
            t.join()

    #define a static method to compress bytes and write it into a single gz file
    @staticmethod
    def combine_csv_gz(des_file_name, csv_bytes_list):
        with open(des_file_name, 'wb') as f_out:
            f_out.write(gzip.compress(b''.join(csv_bytes_list)))
            print(f"Combined {des_file_name}")

class M03A(Tisvcloud):
    def __init__(self, month_str):
        super().__init__(month_str, self.__class__.__name__ )

class M04A(Tisvcloud):
    def __init__(self, month_str):
        super().__init__(month_str, self.__class__.__name__ )

class M05A(Tisvcloud):
    def __init__(self, month_str):
        super().__init__(month_str, self.__class__.__name__ )

if __name__ == '__main__':
    c1 = M03A('2016-11')
    c1.gather_file()

