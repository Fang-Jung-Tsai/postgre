import sqlite3
import pandas as pd
data_path = r'C:\Users\jitin\Desktop\Rosa.database\0918_emic.sqlite'

#read data from sqlite table (rowdata_county) as dataframe
conn = sqlite3.connect(data_path)
df = pd.read_sql_query("SELECT * FROM rowdata_county", conn)
# filter row  WHERE County = '花蓮縣'  from df
df_hualien = df[df['County'] == '花蓮縣']

# read data from sqlite table (rowdata_county) as dataframe
conn.close()