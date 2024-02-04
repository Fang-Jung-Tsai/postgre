import os
import pandas as pd
import numpy as np
import  json
import datetime
import time
import requests
import hashlib
import logging
import PBS_postgre_history as pph
from PBS_update_df import update_df

# create logger
logger = logging.getLogger("web_test_crawler")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.expanduser("~/postgre/practice/log/web_test_crawler.log"))
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

lastestData_pth = os.path.expanduser("~/postgre/practice/lastestData.csv")

url = "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=36384FA8-FACF-432E-BB5B-5F015E7BC1BE"

def crawl_data():
    # it is neccessary to use Taiwan IP to crawl data
    try:
        data = requests.get(url)
        data = data.json()
        
        df = pd.DataFrame.from_dict(data)
        
        df["x1"] = df["x1"].replace(to_replace = "", value = "0.0")
        df["y1"] = df["y1"].replace(to_replace = "", value = "0.0")
        df["srcdetail"] = df["srcdetail"].replace(to_replace = "", value = "無")
        df["direction"] = df["direction"].replace(to_replace = "", value = "無")
        df["roadtype"] = df["roadtype"].replace(to_replace = "", value = "其他")

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
        
    except Exception as e:
        logger.error(e)
    
    return df

if __name__ == '__main__':
        
    if os.path.exists(lastestData_pth):
        lastestData = pd.read_csv(lastestData_pth)
    else:
        lastestData = pd.DataFrame()
    
    try:
        df = crawl_data()
        logger.info(f"[Crawl] Succeed to crawl PBS data")

        if len(lastestData):
            lastestData = update_df(lastestData, df)
        else:
            lastestData = df

    except Exception as e:
        logger.error(f"[Crawl] {e}")
        
    lastestData.to_csv(lastestData_pth, encoding="utf-8-sig", index=False)

    to_upload = sum(lastestData["upload"] == False)
    if to_upload > 0:
        logger.info(f"[Crawl] upload {to_upload} data to postgre")
        res = pph.job(lastestData.copy())

        if res:
            lastestData["upload"] = [True] * len(lastestData)
            lastestData.to_csv(lastestData_pth, encoding="utf-8-sig", index=False)

    else:
        logger.info("[Crawl] no new data")