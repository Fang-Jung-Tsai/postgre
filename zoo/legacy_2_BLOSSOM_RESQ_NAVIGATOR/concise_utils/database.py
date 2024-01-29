#coding=utf-8
###########################################################################
##### database_1, adapter for spatialite database
# 2021May 25 version stage3
#Last modified: 20230604 by Cheng-Tao Yang
import os
import re
import pandas
import sqlite3
import geopandas

from .string import string as nsgstring
from .user_argument import configuration
from .user_argument import baseobject
###########################################################################
## entity is a adapter class between (table/view) and pandas.DataFrame
class entity:
    # entity/alias is a table (proxy) and adapter for dataframe
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
                dataframe   = pandas.read_sql(f'select (rowid -1) as primkey, * from {self.table_name}', self.parent_connect, index_col='primkey')
            else:
                dataframe   = pandas.read_sql(f'select {arg_primkey} as primkey, * from {self.table_name}', self.parent_connect, index_col='primkey')

        return dataframe


###########################################################################
## database_1, adapter for sqlite3 and pandas.DataFrame
class database_1(baseobject):

    def __init__(self , database_path = None):
        import sqlite3
        super().__init__()

        if database_path is None:
            self.database_path = ':memory:'
        else:
            self.database_path = database_path

        try:
            self.connect        = sqlite3.connect(self.database_path)  
            self.cursor         = self.connect.cursor()
            self.attach_db_set = set() 

        except sqlite3.Error as e:
            self.connect        = None  
            self.cursor         = None
            self.alias_count    = None
            print('''An error occurred:_1''', e.args[0])

    def __del__(self):
         if self.connect is not None:
            self.connect.commit()
            self.connect.close()

    def __enter__(self):
        return self
    
    def __exit__(self, type, msg, traceback):
        if type:
            print(msg)
            traceback()

        if self.connect is not None:
            self.connect.commit()

        return False

    def vacuum (self):
        if self.connect is not None:
            self.connect.execute ( '''vacuum''' )
        return 

    def attach_database (self, database_filename:str):

        database_alias = f'attach_db_{nsgstring.alnum_uuid()}' 
        self.attach_db_set.add(database_alias)
        comm = '''attach database '{0}' as {1} ''' . format (database_filename,  database_alias ) ;
        self.connect.execute(comm)
        return database_alias

    def detach_database (self, database_alias:str ):
        
        comm = '''detach database '{0}' ''' . format ( database_alias ) ;
        self.connect.execute( comm )
        self.attach_db_set.remove(database_alias)
    
    def backup(self, filename = None):
        #get uuid
        uuid = nsgstring.alnum_uuid()

        #basename = os.path.basename(self.database_path)
        basename = 'memory' if self.database_path == ':memory:' else os.path.basename(self.database_path)   
        basename = os.path.splitext(basename)[0]

        # Use regular expression to remove non-alphanumeric characters
        pattern = r'[^a-zA-Z0-9_]'
        basename = re.sub(pattern, '', basename)

        #backup_filename = f'{self.factory}/{basename}.backup.{uuid}.sqlite'
        backup_filename = filename if filename is not None else f'{self.factory}/{basename}.backup.{uuid}.sqlite'

        targetdb = database_1(backup_filename)
        self.connect.backup(targetdb.connect, pages=20, sleep=0.0001)
        del targetdb
        return backup_filename
    
    def clone_database (self, srcdb):
        srcdb.connect.backup(self.connect, pages=20, sleep = 0.001)


    def entities (self):
        return pandas.read_sql_query('''select  name, type, sql from sqlite_master where type in ('table', 'view') ''', self.connect, index_col=['name'])

    def tables (self):
        return pandas.read_sql_query('''select  name, type, sql from sqlite_master where type in ('table', 'view') ''', self.connect, index_col=['name'])

    #entity/alias is a table (proxy)
    def get_entity(self, tab_name:str):
        return entity(self.connect, tab_name)
    
    #entity/alias is a table (proxy)
    def get_alias(self, tab_name:str):
        return entity(self.connect, tab_name)

    def touch(self):
        import datetime
        dataframe = pandas.DataFrame.from_dict({'mtime':[datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")], 'path':[self.database_path]} )
        self.get_entity('ctao3_touch').write(dataframe)    

    def drop_entities(self, re_pat_list ) :
        ##debug 1 critical check point 1 
        #drop entities by list of regular expression

        aliases = self.entities()
        for name in aliases.index:
            for re_parttern in re_pat_list:
                if aliases.loc[name, 'type'] == 'table' and re.match(re_parttern, name):
                    self.connect.execute( f'''drop TABLE main.{name}''' )

                elif aliases.loc[name, 'type'] == 'view' and re.match(re_parttern, name):
                    self.connect.execute( f'''drop view main.{name}''' )

        self.connect.commit()                

    def clone_entities(self, re_pat_list : str, source ) :
        ##debug 2 critical check point 2
        aliases = source.entities()
        srcdb   = self.attach_database(source.database_path)

        self.drop_entities(re_pat_list)
        for name in aliases.index:
            for re_parttern in re_pat_list:
                if aliases.loc[name, 'type'] == 'table' and re.match(re_parttern, name):
                    self.connect.execute( f'CREATE TABLE main.{name} as select * from {srcdb}.{name}' )
                    print (f'CLONE {name} from  {source.database_path}')

                elif aliases.loc[name, 'type'] == 'view' and re.match(re_parttern, name):
                    self.connect.execute( aliases.loc[name, 'sql'] )
                    print (f'CLONE view_schema {name} from {source.database_path}')

        self.connect.commit()                
        self.detach_database(srcdb)

    def rename_tables (self, re_pattern : str, re_sub_pattern : str):
        #help to rename table name
        #arguments : des_db.rename_tables('^G97_(.*)_U0200_2015','mineral_nsg_u02000_\g<1>')
        aliases = self.entities()

        for tab_name in aliases.index:
            if re.match(re_pattern, tab_name) and aliases.loc[tab_name,'type'] =='table':
                new_name = re.sub(re_pattern, re_sub_pattern, tab_name)
                self.connect.execute ( f'''ALTER TABLE {tab_name}  RENAME TO {new_name}''' )

                print(f'''ALTER TABLE {tab_name}  RENAME TO {new_name}''')   

    def rename_column(self, tab_name : str, current_col :str, new_col : str ):
        #help to rename column name
        aliases = self.entities()
        
        if re.match ('^Windows', self.platform) and tab_name in aliases.index and aliases.loc[tab_name,'type'] =='table':
            #this is a bug in sqlite3, it does not support rename column
            #we just can user this function (on windows platform) with sqlite3 version 3.25.0 or higher
            try:
                self.connect.execute(f'''ALTER TABLE {tab_name} RENAME COLUMN {current_col} TO {new_col};''')

                print(f'''ALTER TABLE {tab_name} RENAME COLUMN {current_col} TO {new_col};''')

            except sqlite3.OperationalError as e:
                
                print(f'''ALTER TABLE {tab_name} RENAME COLUMN {current_col} TO {new_col};''')
                print(e)
        else:
            print('rename_column is not supported on this platform or table does not exist')


###########################################################################
## spatialite adapter class
#help to create/alter/manage spatialite database, spatialite table and  geometry column.
class database_2(database_1):
    #the function depends on spatialite module 
    def __init__(self , database_path = None):
        
        import sqlite3
        super().__init__(database_path)

        try:
            #check if self.connect has attribute enable_load_extension
            #if not, raise an exception

            if not hasattr(self.connect, 'enable_load_extension'):
                raise sqlite3.NotSupportedError

            self.connect.enable_load_extension(True)
            if self.platform[:7] == 'Windows':
                self.connect.load_extension('mod_spatialite') 
            else:
                self.connect.load_extension('libspatialite')
            
            #the flag to indicate spatialite is enabled
            self.spatialite_enabled = True
            print('''database_2: spatialite is available now''')

        except sqlite3.Error as e:
            #the flag to indicate spatialite is disabled
            self.spatialite_enabled = False
            print('''An error occurred_ database_2:''', e.args[0])

        self.nsg_prefix = '^(schema)|(metadata)|(rawdata)|(mineral)|(argu)|(calc)|(rdset)'

        self.geom_type_dict = {'POINT' : 'POINT', 'LINESTRING' : 'LINESTRING', 'POLYGON' : 'POLYGON',
                               'MULTIPOINT' : 'MULTIPOINT', 'MULTILINESTRING' : 'MULTILINESTRING', 'MULTIPOLYGON' : 'MULTIPOLYGON', 
                               'POINT Z': 'POINT', 'LINESTRING Z' : 'LINESTRING', 'POLYGON Z' : 'POLYGON',
                               'MULTIPOINT Z' : 'MULTIPOINT', 'MULTILINESTRING Z' : 'MULTILINESTRING', 'MULTIPOLYGON Z' : 'MULTIPOLYGON'
                              }
        
        self.taboo_list = '^(ElementaryGeometries)'

    def isgeomdb(self):
        #spatialite geometry fuction (check)
        return True if 'spatial_ref_sys' in self.entities().index else False

    def init_spatialmetadata(self):
        #spatialite geometry fuction (init)
        if not self.isgeomdb() and self.spatialite_enabled:
            self.cursor.execute ('select initspatialmetadata();')
            self.connect.commit()

    def recovergeometrycolumn(self):
        #spatialite geometry fuction (recover)
        
        schema  =     self.get_schema()
        df_gc = self.get_entity('geometry_columns').read('f_table_name')
       
        for tab_name, tab_info in schema.items():
            #spatialite geometry fuction (recover)
            if  self.spatialite_enabled \
                and tab_info['type'] == 'table'  \
                and tab_name.lower() not in df_gc.index  \
                and 'geom' in tab_info \
                and tab_info['geom']['type_i'] is not None\
                and tab_info['geom']['srid'] != 0:
                
                geom_info = tab_info['geom']
                sql       = f'''SELECT RecoverGeometryColumn('{tab_name}', 'geom', {int(geom_info['srid'])}, 
                                '{geom_info['type_i']}', '{geom_info['dimension']}' );'''
                
                self.cursor.execute(sql)
                self.connect.commit()
                print   ( sql )

            else:
                print (f'geometry column of {tab_name} is not recovered')

        self.connect.commit()

    def get_schema(self):
        #spatialite geometry fuction (check)
        schema = {}
        self.init_spatialmetadata()
        df_tabs = self.entities() 

        #remove rows with name in taboo_list
        df_tabs = df_tabs[~df_tabs.index.str.match(self.taboo_list)]
       
        for tab_name, row in df_tabs.iterrows():
            df_tab_info = pandas.read_sql_query(f'''pragma table_info('{tab_name}')''', self.connect, index_col=['name'])

            schema[tab_name] = {'type': row['type'],
                                        'sql' : row['sql'],
                                        'cols_name': df_tab_info.index.tolist(), 
                                        'cols_type': df_tab_info.type.tolist() }
            
            # spatialite geometry fuction (check)
            if self.spatialite_enabled and 'geom' in df_tab_info.index:
                sql = f'''SELECT Count(*) as num, GeometryType("geom") as type, Srid("geom") as srid, 
                            CoordDimension("geom") as dimension
                            FROM "{tab_name}" GROUP BY 2, 3, 4''' 

                df_geom       = pandas.read_sql(sql, self.connect)
                geom_info     = df_geom.iloc[ df_geom['num'].idxmax() ]
                schema[tab_name]['geom'] = geom_info.to_dict()

                if 'type' in schema[tab_name]['geom'] and schema[tab_name]['geom']['type'] is not None:
                    schema[tab_name]['geom']['type_i'] = self.geom_type_dict [schema[tab_name]['geom']['type']]
                else:
                    schema[tab_name]['geom']['type_i'] = None

        return schema


    def get_nsg_schema(self):        

        schema = self.get_schema()
        sechma = {key: value for key, value in schema.items() if re.match(self.nsg_prefix, key)  }
        return sechma 

    def add_columns(self, tab_name : str, col_list ) :
        #help to add columns to a specific table
        schema = self.get_schema()

        for col in set(col_list):
            if tab_name in schema and col not in schema[tab_name]['cols_name']:    
                self.connect.execute(f'ALTER TABLE {tab_name} ADD {col}')
                print (f'''field "{col}" added to table "{tab_name}"''')

            else: 
                print (f'''field "{col}" already exists or table name "{tab_name}" is malformed''')

    def drop_columns(self, tab_name : str, col_list ) :
        #help to drop columns from a specific table
        schema = self.get_schema()

        if tab_name in schema:    
            entity1 = self.get_entity( tab_name )
            entity1.write(entity1.read().drop(columns=col_list))
            print (f'''field "{col_list}" dropped from table "{tab_name}"''')   
            
        else: 
            print (f'''table name "{tab_name}" is malformed''')


    def get_geopandas(self, table_name):
        import shapely

        db_schema = self.get_schema()
        if table_name not in db_schema:
            return None

        if 'geom' not in db_schema[table_name]['cols_name']:
            pdf = self.get_entity(table_name).read()
            #convert pdf to gdf without geometry
            #gdf = geopandas.GeoDataFrame(pdf, geometry=None)
            return pdf
        
        dataframe   = pandas.read_sql(f'select asbinary(geom) as geometry,  * from {table_name}', self.connect)
    
        #use Shapely to convert geometry column (wkt) to a geodaframe geometry objects'
        dataframe['geometry'] = dataframe['geometry'].apply(lambda x: shapely.wkb.loads(x))

        schema = db_schema[table_name]
        coltype = zip(schema['cols_name'], schema['cols_type'])   


        #drop  if exits geom column 
        if 'geom' in dataframe.columns:
            dataframe = dataframe.drop(columns=['geom'])

        #convert text in dataframe to str 
        for col, type in coltype:
            if col != 'geom' and type == 'TEXT' :
                dataframe[col] = dataframe[col].astype(str)

        #convert dataframe to geodataframe
        dataframe = geopandas.GeoDataFrame(dataframe, geometry='geometry') 

        #get geom srid from schema and set it to geodataframe
        srid = schema['geom']['srid']        
        dataframe.crs = {'init' :'epsg:' + str(srid)}

        return dataframe
        
    @staticmethod
    def leftjoin(gdf_a, gdf_b, on_condition):
    
        #perform the join
        df_c = geopandas.sjoin(gdf_a, gdf_b, how='left', op='intersects')
        #print (df_c)
        return df_c


############################################################################################################
##alias list for classes and functions in this module
manidb = database_2
alias  = entity


