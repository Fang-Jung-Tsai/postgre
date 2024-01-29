#//# coding=utf-8
# 2021May 25 version stage3
 
import os
import sqlite3
import pandas
import numpy
import os, socket, platform, datetime

from .ctao2_42 import TODEL 
from .ctao2_nsgstring import alnum_uuid
from .ctao2_hostmetadata import hostmetadata

class database:

    def __init__(self , database_path):
        self.uuid               = alnum_uuid()
        self.host               = hostmetadata()
        self.platform           = platform.platform()[:6]  
        self.database_path      = database_path

        try:
            self.connect            = sqlite3.connect(database_path)  
            self.connect.enable_load_extension(True)
            self.cursor             = self.connect.cursor()
            self.alias_count        = 0

            if self.platform == 'Window':
                self.connect.load_extension('mod_spatialite') 
            else:
                self.connect.load_extension('libspatialite')

        except sqlite3.Error as e:
            self.connect            = None  
            self.cursor             = None
            self.alias_count        = None
            print('''An error occurred:''', e.args[0])

    def __del__(self):
        self.connect.commit()
        self.connect.close()

    def __enter__(self):
        return self
    
    def __exit__(self, type, msg, traceback):
        if type:
            print(msg)       
        self.connect.commit()
        return False

    def vacuum (self):        
        self.connect.execute ( '''vacuum''' )
        return 

    def attach_database (self, database_filename , database_alias ):
        self.alias_count = self.alias_count + 1 
        comm = '''attach database '{0}' as {1} ''' . format (database_filename,  database_alias ) ;
        self.connect.execute( comm )

    def detach_database (self, database_alias ):
        self.alias_count = self.alias_count + 1 
        comm = '''detach database '{0}' ''' . format ( database_alias ) ;
        self.connect.execute( comm )

#
#   function for getting shape of database
#
    def tables (self):
        return pandas.read_sql_query('''select  name, type, sql from sqlite_master where type in ('table', 'view') ''', self.connect, index_col=['name'])



#
#   Group
#   Excel file
#
#   
    #def fit_xlsx(self, dataframe):

    #        #dtype.num
    #        #A unique number for each of the 21 different built-in types.
    #        # 19 str
    #        # 12 float
    #    if dataframe.shape[0] <= 1012345 and dataframe.shape[1]  <= 6123 :
    #        for col, dtype in zip(dataframe.columns, dataframe.dtypes):
    #            print (col, dtype, dtype.num)
    #            if  dtype.num == 17 : 
    #                dataframe[col] = dataframe[col].apply(lambda x : x if  isinstance(x, str) and len(x) < 3000 else None)

    #        return dataframe
    #    else:
    #        return pandas.DataFrame()

    #def to_xlsx (self, tablename_re_parttern='(argu_|calc_|data_).*', xlsx_filename=None):

    #    import re, pandas
    #    from Lily.blacksmith.file_feature import get_feature

    #    if xlsx_filename is None:
    #        feature = get_feature(self.database_path)
    #        xlsx_filename = feature['path'] + '/' + feature['name'] + '_lily.xlsx'

    #    writer    = pandas.ExcelWriter(xlsx_filename, engine = 'xlsxwriter')

    #    for table_name in self.tables().index:

    #        if re.match(tablename_re_parttern, table_name) and len(table_name) < 32:
    #            dataframe = self.to_dataframe(table_name) 
    #            dataframe = self.fit_xlsx(dataframe)
    #            if dataframe.empty is not True:
    #                dataframe.to_excel(writer, index_label='pd_idx', sheet_name=table_name)

    #    writer.save()
    #    writer.close()
    #    return 
        
    #def update_from_xlsx (self, xlsx_filename=None):

    #    import re, pandas
    #    from Lily.blacksmith.file_feature import get_feature

    #    if xlsx_filename is None:
    #        feature = get_feature(self.database_path)
    #        xlsx_filename = feature['path'] + '/' + feature['name'] + '_lily.xlsx'

    #    ddict = pandas.read_excel(xlsx_filename, sheet_name=None, index_col=0)

    #    for tablename in ddict:
    #        dataframe_1 = ddict[tablename]
    #        self.to_table(tablename + '_lily_xlsx', dataframe_1)

    #    return dict