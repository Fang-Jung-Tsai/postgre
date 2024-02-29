import sys
my_package_path = os.path.expanduser('~/postgre/zoo')
sys.path.append(my_package_path)

import os
import pandas as pd
import numpy as np
import requests
import hashlib
import  json
import datetime
import time
import logging
from argparse import ArgumentParser
import feedparser

from postgis_CE13058 import postgis_CE13058

class postgis_handler():
    def __init__(self, history_table, key_table) -> None:
        self.db = postgis_CE13058()
        self.history_table = history_table
        self.key_table = key_table
        self.key_tmp_table = key_table + "_tmp"

    def add(self, df, table):
        self.db.append_data(df, table)


class webCrawler():

    def __init__(self, url, output_fn, log_fn):
        self.url = url
        self.output_fn = output_fn

        self.log_fn = log_fn
        self.df = pd.DataFrame()
        
        self.set_logger()

    def set_postgis(self, history_table, key_table):
        self.postgis = postgis_handler(history_table, key_table)

    def update_df(self, df1, df2):

        continue_incidents = df1.merge(df2, on=['UID'], how='right', indicator=True, suffixes=('', '_y')).query('_merge == "both"')
        continue_incidents['upload'] = continue_incidents['upload'].astype(bool)

        new_incidents = df2.merge(df1, on=['UID', 'hash'], how='left', indicator=True, suffixes=('', '_y')).query('_merge == "left_only"')
        new_incidents['upload'] = new_incidents['upload'].astype(bool)

        final_df = pd.concat((continue_incidents, new_incidents)) if len(new_incidents) else continue_incidents
        _col = [col for col in final_df.columns if '_y' in col] + ['_merge']
        final_df = final_df.drop(_col + ['_merge'], axis=1)
        final_df.reset_index(drop=True, inplace=True)

        return final_df
    
    
    def set_logger(self):
        self.logger = logging.getLogger("web_crawler")
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(os.path.expanduser(self.log_fn))
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def save_csv(self):
        self.df.to_csv(self.output_fn, encoding="utf-8-sig", index=False)


class PBSCrawler(webCrawler):
    def __init__(self, url, output_fn, log_fn, 
                 history_tb_name="data_rosa_fj_pbs",
                 key_tb_name="data_rosa_fj_pbs_key"):
        super.__init__(url, output_fn, log_fn)
        self.set_postgis(history_tb_name, key_tb_name)

    def crawl_data(self):
        # it is neccessary to use Taiwan IP to crawl data
        try:
            data = requests.get(self.url)
            data = data.json()
            
            df = pd.DataFrame.from_dict(data)
            
            df["x1"] = df["x1"].replace(to_replace = "", value = "0.0")
            df["y1"] = df["y1"].replace(to_replace = "", value = "0.0")
            df["srcdetail"] = df["srcdetail"].replace(to_replace = "", value = "None")
            df["direction"] = df["direction"].replace(to_replace = "", value = "None")
            df["roadtype"] = df["roadtype"].replace(to_replace = "", value = "None")

            df["happendate"] = df["happendate"].apply(lambda x: x.replace("-", "/"))
            df["happentime"] = df["happentime"].apply(lambda x: x[:8])
            df["datetime"] = df["happendate"] + "-" + df["happentime"]

            # create hash key of comment
            hash_keys =[]
            for i in range(len(df)):
                hash = hashlib.md5(bytes(df.iloc[i]["comment"], 'utf-8'))
                hash_keys.append(hash.hexdigest())
            df["hash"] = hash_keys
            df["upload"] = [False] * len(df)

            if len(self.df):
                self.df = self.update_df(self.df, df)
            else:
                self.df = df

            self.save_csv()

        except Exception as e:
            self.logger.error(e)

    def update_db(self):
        now = datetime.datetime.now()
        nowDttm = now.strftime("%Y/%m/%d-%H:%M:%S")
        unix_time = int(now.timestamp() * 1000)
        df = self.df.copy()
        df['downloaddttm'] = unix_time

        df = df.rename(columns={'UID': 'uid', 'modDttm': 'moddttm', 'areaNm': 'areaname'})

        try:
            self.add_history()
        except Exception as e:
            self.logger.error(f'[Histroy] {e}')

        group_df = df.groupby(['uid'])
        count_key = 0

        for id, gp in group_df:
            gp = gp.sort_values(by="moddttm", ascending=True)
            
            all_num = len(gp)
            uploaded_num = sum(gp['upload'] == True)
            non_uploaded_num = sum(gp['upload'] == False)

            tmp_df = self.get_key_df(gp)

            if all_num == non_uploaded_num:
                self.postgis.add(tmp_df, self.postgis.key_table)
                count_key += 1

            elif uploaded_num >= 1 and non_uploaded_num > 0:
                self.postgis.add(tmp_df, self.postgis.key_table)

        tmp_df = self.postgis.read_data(self.postgis.key_tmp_table)

        if len(tmp_df):
            try:
                query = f"""
                    UPDATE {self.postgis.key_table}
                    SET moddttm = {self.postgis.key_tmp_table}.moddttm,
                        hash = {self.postgis.key_tmp_table}.hash,
                        downloaddttm = {self.postgis.key_tmp_table}.downloaddttm
                    FROM tmp_table
                    WHERE {self.postgis.key_table}.uid = {self.postgis.key_tmp_table}.uid;
                """
                count_key += self.postgis.update_data(query)
                self.logger.info(f'Update {count_key} data into key table')
                self.postgis.drop_table(self.postgis.key_tmp_table)
            
            except Exception as e:
                self.logger.error(f'[Update tmp_df] {e} {tmp_df}')

    def add_history(self):
        df = df.df[df.df['upload'] == False]
        df = df.drop(columns=['upload'])
        
        self.postgis.add(df, self.postgis.history_table)
   
    
    def get_key_df(self, df):
        df = df[["uid", "hash", "datetime", "moddttm", "downloaddttm"]]

        tmp_df = {
            "uid": df.iloc[0]["uid"],
            "datetime" : df.iloc[0]["datetime"],
            "moddttm": [df["moddttm"].drop_duplicates().tolist()],
            "hash": [df["hash"].drop_duplicates().tolist()],
            "downloaddttm": df.iloc[0]["downloaddttm"]
        }

        return pd.DataFrame(tmp_df)
        
    
class NewsCrawler(webCrawler):
    def __init__(self, url, output_fn, log_fn):
        super.__init__(url, output_fn, log_fn)
        self.postgis.history_table = "data_rosa_fj_wsj"
        self.postgis.key_table = "data_rosa_fj_wsj_key"

    def crawl_data(self):
        # it is neccessary to use Taiwan IP to crawl data
        try:
            feed = feedparser.parse(self.url)
            df = pd.DataFrame(feed.entries)
            df = df[['title', 'link', 'summary', 'published']]
            df['published'] = df['published'].map(self.format_datetime) 

        except Exception as e:
            self.logger.error(e)
        
        return df
    
    def format_datetime(self, datetime_string):
        converted_datetime = datetime.strptime(datetime_string, "%a, %d %b %Y %H:%M:%S %z")
        formatted_datetime = converted_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        return formatted_datetime
    
    def custom_filter(self, row, decreased_datetime):
        converted_datetime = datetime.strptime(row['published'], "%Y-%m-%d %H:%M:%S")
        return converted_datetime > decreased_datetime
