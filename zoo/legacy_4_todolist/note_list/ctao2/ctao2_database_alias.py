# 2021May 25 version stage3

import re
import os
import json
import shutil
import pandas
from datetime import datetime
from .ctao2_database_2 import database_2
from .ctao2_nsgstring import alnum_uuid
from .ctao2_hostmetadata import hostmetadata

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

    def replace(self, dataframe):
        self.write(dataframe, 'replace')

    def append(self, dataframe):
        self.write(dataframe, 'append')

    def write(self, dataframe, if_exists_arg='replace'):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists=if_exists_arg, index = False)

    def write_with_index(self, dataframe):
        dataframe.to_sql(self.table_name, self.parent_connect, if_exists='replace', index = True)

    def read(self, arg_primkey = None):
        check_sql   = '''SELECT name FROM sqlite_master WHERE type in ('table', 'view') AND name='{0}' '''.format(self.table_name);
        check_df    = pandas.read_sql(check_sql, self.parent_connect,)
        dataframe   = pandas.DataFrame()

        if not check_df.empty :
            if arg_primkey is None:
                dataframe   = pandas.read_sql(f'select (rowid -1) as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')
            else:
                dataframe   = pandas.read_sql(f'select {arg_primkey}  as nsg_row_primkey, * from {self.table_name}', self.parent_connect, index_col='nsg_row_primkey')

        return dataframe

class manidb (database_2):

    def __init__(self,  sourcedb_name):

        super().__init__(sourcedb_name) 

    def transaction (self, SQLtransaction_list):
        try:
            for sql in SQLtransaction_list:
                self.connect.execute(sql)

        except self.connect.Error:
            self.connect.rollback()
 
        except:
            self.connect.rollback()
 
    def get_alias(self, table_name):
        return alias (self.connect, table_name)

    def read_sql (self, sql : str) -> pandas.DataFrame :
        data_fm     = pandas.read_sql(sql,  self.connect)
        return data_fm



    def read_table(self, _tablename):
        return pandas.read_sql_query('select * from ' + _tablename , self.connect)

    def overwrite_table(self, _tablename, _dataframe):
        _dataframe.to_sql(_tablename, self.connect, if_exists='replace', index=True)


    def backup(self, filename = None):

        #backup database_target to warehouse 
        basename = os.path.basename(self.database_path)

        backup_filename = f'''{self.host.factory}/manidb.backup.{basename}'''  if filename is None else filename  

        targetdb = database_2(backup_filename)
        self.connect.backup(targetdb.connect, pages=20, sleep=0.0001)
        del targetdb
        return backup_filename

    def clone_aliases_data (self, re_pat_list : str, source_db_path : str):
        
        srcdb   = manidb(source_db_path)

        for tab_name in srcdb.tables().index:
            for re_parttern in re_pat_list:
                if re.match(re_parttern, tab_name):
                    df  = srcdb.get_alias(tab_name).read()
                    self.get_alias(tab_name).write(df)
                    print (f'''CLONE {tab_name} data from {source_db_path}''')

        del srcdb