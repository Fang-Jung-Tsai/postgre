import shutil, os, requests, geopandas
from multiprocessing import Pool
# 用於處理每筆資料的函數

def worker(row):
    # 取得欄位 A 和 B 的值，並進行相除運算
    result = row['cnt_left'] / row['area']
    return result


if __name__ == '__main__':
    import re
    import os
    import pandas
    import fiona
    from lily.core import user_argument
    from lily.database import manidb
    from matplotlib import pyplot as plt
    from geopandas import GeoDataFrame
    from shapely.geometry import Point

    conf = user_argument()
    
    rosa_argu    = os.path.join(r'C:\Users\jitin\Desktop\ROSA.database','ROSA.common.gpkg')
    #read geopackage table 'argu_town' from rosa_argu
    argu_town    = geopandas.read_file(rosa_argu, driver='GPKG', layer='argu_town')
    #select columns from argu_town ['cnty_code', 'town_code' ,'cnty_name','town_name','geometry']
    argu_town    = argu_town[['cnty_code', 'town_code' ,'cnty_name','town_name', 'geometry']]

    argu_town.plot()
    plt.show()


   

