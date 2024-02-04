import os
import pandas as pd
import geopandas
import numpy as np
import datetime
import time
import sys
my_package_path = os.path.expanduser('~/postgre/zoo')
# Add the path to sys.path
sys.path.append(my_package_path)

from postgis_CE13058 import postgis_CE13058

import PBS_web_crawler as pwc

import decimal
import logging
from argparse import ArgumentParser

# create logger
logger = logging.getLogger('road_test_crawler')
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
fh = logging.FileHandler(os.path.expanduser("~/postgre/practice/log/road_test_crawler.log"))
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)


def add_history(db, table, df):
    df.drop(columns=['upload'])
    try:
        db.append_data(df, table)
        
    except Exception as e:
        logger.error(f"[add_history] {e} {df}")

def update_key(db, table, df):

    df = df[["uid", "hash", "datetime", "moddttm", "downloaddttm"]]

    tmp_df = {
        "uid": df.iloc[0]["uid"],
        "datetime" : df.iloc[0]["datetime"],
        "moddttm": [df["moddttm"].drop_duplicates().tolist()],
        "hash": [df["hash"].drop_duplicates().tolist()],
        "downloaddttm": df.iloc[0]["downloaddttm"]
    }

    try:
        tmp_df = pd.DataFrame(tmp_df)
        db.append_data(tmp_df, table)
        logger.info(f"[update_key] {df.iloc[0]['uid']}")

    except Exception as e:
        logger.error(f"[update_key] {e} {tmp_df}")

def add_key(db, table, df):

    df = df[["uid", "hash", "datetime", "moddttm", "downloaddttm"]]
    
    tmp_df = {
        "uid": df.iloc[0]["uid"],
        "datetime" : df.iloc[0]["datetime"],
        "moddttm": [df["moddttm"].drop_duplicates().tolist()],
        "hash": [df["hash"].drop_duplicates().tolist()],
        "downloaddttm": df.iloc[0]["downloaddttm"]
    }
    
    tmp_df = pd.DataFrame(tmp_df)

    try:
        db.append_data(tmp_df, table)
        logger.info(f"[add_key] {df.iloc[0]['uid']}")

    except Exception as e:
        logger.error(f"[add_key] {e} {tmp_df}")            

def job(df):

    now = datetime.datetime.now()
    nowDttm = now.strftime("%Y/%m/%d-%H:%M:%S")
    unix_time = int(now.timestamp() * 1000)
    df['downloaddttm'] = unix_time

    df = df.rename(columns={'UID': 'uid', 'modDttm': 'moddttm', 'areaNm': 'areaname'})

    postgis = postgis_CE13058()
    history_table = 'data_rosa_fj_pbs'
    key_table = 'data_rosa_fj_pbs_key'
    
    try:
        add_history(db=postgis, table=history_table, df=df[df['upload'] == False])
        logger.info(f'Finish insert {len(df[df["upload"] == False])} data into history table')
        
    except Exception as e:
        logger.error(f'[Histroy] {e}')
    
    time.sleep(3)

    group_df = df.groupby(['uid'])
    count_key = 0

    for id, gp in group_df:
        gp = gp.sort_values(by="moddttm", ascending=True)
        
        all_num = len(gp)
        uploaded_num = sum(gp['upload'] == True)
        non_uploaded_num = sum(gp['upload'] == False)

        if all_num == non_uploaded_num:
            add_key(db=postgis, table=key_table, df=gp)
            count_key += 1

        elif uploaded_num >= 1 and non_uploaded_num > 0:
            update_key(db=postgis, table="tmp_table", df=gp)

    tmp_df = postgis.read_data("tmp_table")

    if len(tmp_df):
        try:
            query = f"""
                UPDATE {key_table}
                SET moddttm = tmp_table.moddttm,
                    hash = tmp_table.hash,
                    downloaddttm = tmp_table.downloaddttm
                FROM tmp_table
                WHERE {key_table}.uid = tmp_table.uid;
            """
            count_key += postgis.update_data(query)
            logger.info(f'Update {count_key} data into key table')
            postgis.drop_table('tmp_table')
        
        except Exception as e:
            logger.error(f'[Update tmp_df] {e} {tmp_df}')
                
    return True

if __name__ =='__main__':

    lastestData_pth = os.path.expanduser('~/postgre/practice/lastestData.csv')
    df = pd.read_csv(lastestData_pth)
    df = df.reset_index(drop=True)
    job(df)