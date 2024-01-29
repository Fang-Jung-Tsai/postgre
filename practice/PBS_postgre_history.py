import os
import pandas as pd
import geopandas
import numpy as np
import datetime
import time
import sys
my_package_path = '~/postgre/zoo'
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
fh = logging.FileHandler("~/postgre/practice/log/road_test_crawler.log")
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)

# create logger
logger2 = logging.getLogger('road_test_crawler')
logger2.setLevel(logging.DEBUG)
# create console handler and set level to debug
fh2 = logging.FileHandler("~/postgre/practice/log/road_missing.log")
fh2.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
fh2.setFormatter(formatter)
# add ch to logger
logger2.addHandler(fh2)


def add_history(db, table, df):
    df.drop(columns=['upload'])
    try:
        db.append_data(df, table)
        
    except Exception as e:
        logger.error(f"[History] {e}")
        logger2.error(f"[History] {df}")


def update_key(db, table, df):

    df = df[["uid", "hash", "datetime", "moddttm", "downloaddttm"]]

    tmp_df = {
        "uid": df.iloc[0]["uid"],
        "datetime" : df.iloc[0]["datetime"],
        "moddttm": [set(df["moddttm"])],
        "hash": [set(df["hash"])],
        "downloaddttm": df.iloc[0]["downloaddttm"]
    }

    try:
        tmp_df = pd.DataFrame(tmp_df)
        db.append_data(tmp_df, "tmp_table")

    except Exception as e:
        logger.error(f"[Update] {df['uid']} - {df['hash']} {e}")
        logger2.error(f"[Update] {tmp_df}")
    


def add_key(db, table, df):

    df = df[["uid", "hash", "datetime", "moddttm", "downloaddttm"]]
    
    tmp_df = {
        "uid": df.iloc[0]["uid"],
        "datetime" : df.iloc[0]["datetime"],
        "moddttm": [set(df["moddttm"])],
        "hash": [set(df["hash"])],
        "downloaddttm": df.iloc[0]["downloaddttm"]
    }
    
    tmp_df = pd.DataFrame(tmp_df)

    try:
        db.append_data(tmp_df, table)

    except Exception as e:
        logger.error(f"[Add] {df['uid']} - {df['hash']} {e}")
        logger2.error(f"[Add] {tmp_df}")
            

def job(df):

    now = datetime.datetime.now()
    nowDttm = now.strftime("%Y/%m/%d-%H:%M:%S")
    unix_time = int(now.timestamp() * 1000)
    df['downloaddttm'] = unix_time

    df = df.rename(columns={'UID': 'uid', 'modDttm': 'moddttm', 'areaNm': 'areaname'})

    postgis = postgis_CE13058()
    history_table = 'data_rosa_fj_pbs'
    key_table = 'data_rosa_fj_pbs_key'
    # postgis.drop_table(history_table)
    # postgis.drop_table(key_table)
    postgis.drop_table('tmp_table')
    
    try:
        add_history(db=postgis, table=history_table, df=df[df['upload'] == False])
        
    except Exception as e:
        logger.error(f'[Histroy] {e}')
    
    logger.info(f'Finish insert {len(df[df["upload"] == False])} data into history table')
    print(f'Finish insert {len(df[df["upload"] == False])} data into history table')

    time.sleep(3)

    group_df = df.groupby(['uid'])
    count_key = 0

    for id, gp in group_df:
        gp = gp.sort_values(by="moddttm", ascending=True)
        
        all_num = len(gp)
        uploaded_num = sum(gp['upload'] == True)
        non_uploaded_num = sum(gp['upload'] == False)

        if all_num == non_uploaded_num:
            print(f'create {gp.iloc[0].uid}')
            add_key(db=postgis, table=key_table, df=gp)
            count_key += 1

        elif uploaded_num >= 1 and non_uploaded_num > 0:
            print(f'update {gp.iloc[0].uid}')
            update_key(db=postgis, table=key_table, df=gp)

    tmp_df = postgis.read_data("tmp_table")
    # print(tmp_df)

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
        
            postgis.drop_table("tmp_table")

        except Exception as e:
            logger.error(f'[Update] {e}')
            logger2.error(f"[Update tmp_df] {tmp_df}")
            

    logger.info(f'Update {count_key} data into key table')

    return True



if __name__ =='__main__':

    lastestData_pth = '~/postgre/practice/lastestData.csv'
    df = pd.read_csv(lastestData_pth) #.iloc[31000:31500]
    # df.loc[31000:31500, "upload"] = True
    df = df.reset_index(drop=True) #.iloc[29000:]
    print(len(df))
    job(df)

    # postgis = postgis_CE13058()
    # # history_table = 'data_rosa_fj_pbs'
    # key_table = 'data_rosa_fj_pbs_key'
    # # df = postgis.read_data(history_table)
    # df = postgis.read_data(key_table)

    # print(df)
