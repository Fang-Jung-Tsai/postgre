import shutil, os, requests, geopandas
from multiprocessing import Pool
# 用於處理每筆資料的函數

def worker(row):
    # 取得欄位 A 和 B 的值，並進行相除運算
    result = row['cnt_left'] / row['area']
    return result

if __name__ == '__main__':
    import os
    import fiona
    import pandas
    import fiona
    from lily.core import user_argument
    from lily.database import manidb
    from matplotlib import pyplot as plt
    from geopandas import GeoDataFrame
    from shapely.geometry import Point

    conf = user_argument()
    
    common_path = os.path.join(conf.factory,'ROSA.common.gpkg')
    heatmap_path = os.path.join(conf.factory,'ROSA.CALC.heatmap.gpkg')

    #read gpkg layer('data_grid500')
    gpdf_500m   = geopandas.read_file(common_path, layer='argu_grid500m') 
    gpdf_city   = geopandas.read_file(common_path, layer='argu_county')
    gpdf_segis  = geopandas.read_file(common_path, layer='argu_segis_A3_109')

    list_city = []
    for city, code in zip(gpdf_city['cnty_name'],gpdf_city['cnty_code']):
        print(city,code)
        calcA = gpdf_city[gpdf_city['cnty_name'] == city]
        calc2 = gpdf_500m.sjoin(calcA, how='inner', predicate='intersects')

        # keep columns ('geometry' , 'Description', 'city, 'code')
        calc2 = calc2[['geometry','Description','cnty_name','cnty_code']]
        calc3 = calc2.sjoin(gpdf_segis, how='left', predicate='intersects')
        
        #calcate the intersection area of grid500m and segis
        #calc3['i_area'] = calc3.intersections.geometry.area

        calc3.to_file(heatmap_path, layer=f'calc_grid500m_{code}', driver='GPKG', index=False)
        list_city.append(calc3)

    calc2 = pandas.concat(list_city)
    calc2 = GeoDataFrame(calc2, geometry='geometry')
    calc2.to_file(heatmap_path, layer='calc_grid500m_sum', driver='GPKG', index=False)