#//# coding=utf-8
import numpy
import pandas
import re, os, socket, platform, datetime
from datetime import datetime
from Lily.ctao2.ctao2_nsgstring import alnum_uuid
from Lily.ctao2.ctao2_hostmetadata import hostmetadata

class tickwatch:

    def __init__(self):
        self.uuid               = alnum_uuid()
        self.host               = hostmetadata()
        self.begtime            = datetime.now()
        self.msgtime            = [['beg', self.begtime, 0]]            

    def tick(self, msg_text='tick'):
        time_point              = datetime.now()
        seconds = (time_point - self.begtime).seconds
        self.msgtime.append( [msg_text, time_point, seconds ] )
        print ( '{1}_({0:06})\t\t'.format(seconds, self.uuid[-12:]), msg_text)

class alias:
    def __init__(self, parent_database_connect, table_name):

        self.table_name         = table_name 
        self.parent_connect     = parent_database_connect
        self.watch              = tickwatch()

    def tick(self, text):
        self.watch.tick(text)

    def replace(self, dataframe):
        self.write(dataframe, 'replace')

    def append(self, dataframe):
        self.write(dataframe, 'append')

    def write(self, dataframe, if_exists_arg='replace'):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists=if_exists_arg, index = False, chunksize = 100)

    def write_with_index(self, dataframe):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists='replace', index = True, chunksize = 100)

    def read(self, arg_primkey = None):
        check_sql   = '''   SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema='public'
                            AND table_type='BASE TABLE' and table_name='{0}' '''.format(self.table_name);

        check_df    = pandas.read_sql(check_sql, self.parent_connect,)
        dataframe   = pandas.DataFrame()

        if not check_df.empty :
            if arg_primkey is None:
                dataframe   = pandas.read_sql(f'select ctid as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')
            else:
                dataframe   = pandas.read_sql(f'select {arg_primkey}  as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')

        return dataframe

class postgresql:
    def __init__(self, database_name:str, hostip:str='127.1.0.1'):
        import sqlalchemy
        self.host       = hostmetadata()
        self.uuid       = alnum_uuid()

        self.connect    = sqlalchemy.create_engine(f'postgres://postgres:6630.vm@{hostip}:5432/{database_name}')

    def get_alias(self, table_name):
        return alias (self.connect, table_name)

    def read_sql (self, sql : str) -> pandas.DataFrame :
        data_fm     = pandas.read_sql(sql,  self.connect)
        return data_fm

    def tables (self) :

        check_sql   = '''SELECT *
                         FROM information_schema.tables
                         WHERE table_schema='public'
                         AND table_type='BASE TABLE' ''' ;

        data_fm     = pandas.read_sql(check_sql, self.connect, index_col =['table_name'])
        return data_fm

    def drop_table (self, table_name : str):
        check_sql = f''' drop TABLE if exists "{table_name}" '''
        self.connect.execute( check_sql )

    def rename_table (self, src_table : str, tar_table : str ):
        '''ALTER TABLE IF EXISTS table_name RENAME TO new_table_name; '''
        self.connect.execute ( f'''ALTER TABLE if EXISTS {src_table}  RENAME TO {tar_table}''' )
        return
 
    def __enter__(self):
        return self
    
    def __exit__(self, type, msg, traceback):
        if type:
            print(msg)       
        self.connect.commit()
        return False
    
if __name__ == '__console__' or __name__ == '__main__':

    pg1 = postgresql('house_tax', '192.192.138.133')


