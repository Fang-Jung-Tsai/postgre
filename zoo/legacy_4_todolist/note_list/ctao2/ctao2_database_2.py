# 2021May 25 version stage3

import re
import pandas
from .ctao2_42 import answer
from .ctao2_database import database

class database_2(database):

    def __init__(self, srcdb_filename):
        super().__init__(srcdb_filename)

        self.nsg_tab_prefix = '^(_T)|(argu)|(calc)|(rdset)|(data)|(metadata)|(schema)|(mineral)|(rawdata)|(A[0-9]{5}(.*))'

        #待確認
        self.geom_type_dict = {'POINT' : 'POINT', 'LINESTRING' : 'LINESTRING', 'POLYGON' : 'POLYGON',
                               'MULTIPOINT' : 'MULTIPOINT', 'MULTILINESTRING' : 'MULTILINESTRING', 'MULTIPOLYGON' : 'MULTIPOLYGON', 
                               'POINT Z': 'POINT', 'LINESTRING Z' : 'LINESTRING', 'POLYGON Z' : 'POLYGON',
                               'MULTIPOINT Z' : 'MULTIPOINT', 'MULTILINESTRING Z' : 'MULTILINESTRING', 'MULTIPOLYGON Z' : 'MULTIPOLYGON'
                              }

    def isgeomdb(self):
        return True if 'spatial_ref_sys' in self.tables().index else False

    def init_spatialmetadata(self):
        if not self.isgeomdb():
            self.cursor.execute ('select initspatialmetadata();')

    def get_schema(self):

        self.init_spatialmetadata()

        self.schema = {}
        
        df_tabs = pandas.read_sql_query('''select name, type, sql from sqlite_master where type in('table', 'view') order by name''', 
                                        self.connect, index_col=['name'])

        for tab_name, row in df_tabs.iterrows():
            
            if re.match (self.nsg_tab_prefix, tab_name ):

                df_tab_info = pandas.read_sql_query(f'''pragma table_info('{tab_name}')''', self.connect, index_col=['name'])

                self.schema[tab_name] = {'type': row['type'],
                                         'sql' : row['sql'],
                                         'cols_name': df_tab_info.index.tolist(), 
                                         'cols_type': df_tab_info.type.tolist() }
            
                if 'geom' in df_tab_info.index:
                    sql = f'''SELECT Count(*) as num, GeometryType("geom") as type, Srid("geom") as srid, 
                              CoordDimension("geom") as dimension
                              FROM "{tab_name}" GROUP BY 2, 3, 4''' 

                    df_geom       = pandas.read_sql(sql, self.connect)
                    geom_info     = df_geom.iloc[ df_geom['num'].idxmax() ]
                    self.schema[tab_name]['geom'] = geom_info.to_dict()

                    if 'type' in self.schema[tab_name]['geom'] :
                        self.schema[tab_name]['geom']['type_i'] = self.geom_type_dict [self.schema[tab_name]['geom']['type']]

        return self.schema

    def recovergeometrycolumn(self):
        #檢查變數是否已經被定義過
        try: 
            self.schema 
        except: 
            self.get_schema()

        self.init_spatialmetadata()

        df_gc = self.get_alias('geometry_columns').read('f_table_name')
       
        for tab_name, tab_info in self.schema.items():
            if  tab_info['type'] == 'table'  \
                and tab_name.lower() not in df_gc.index  \
                and 'geom' in tab_info \
                and tab_info['geom']['srid'] != 0:
                
                geom_info = tab_info['geom']
                sql       = f'''SELECT RecoverGeometryColumn('{tab_name}', 'geom', {int(geom_info['srid'])}, 
                                '{geom_info['type_i']}', '{geom_info['dimension']}' ); \n'''
                

                self.cursor.execute (sql)
                self.connect.commit()
                print   ( sql )

        self.connect.commit()

    def rename_tables (self, re_pattern : str, re_sub_pattern : str): 

        aliases = self.tables()

        for tab_name in aliases.index:
            if re.match(re_pattern, tab_name) and aliases.loc[tab_name,'type'] =='table':
                new_name = re.sub(re_pattern, re_sub_pattern, tab_name)
                self.connect.execute ( f'''ALTER TABLE {tab_name}  RENAME TO {new_name}''' )

                print(f'''ALTER TABLE {tab_name}  RENAME TO {new_name}''')   

    def rename_column(self, tab_name : str, current_col :str, new_col : str ) :

        aliases = self.tables()

        if tab_name in aliases.index and aliases.loc[tab_name,'type'] =='table':
            self.connect.execute(f'''ALTER TABLE {tab_name} RENAME COLUMN {current_col} TO {new_col};''')

            print(f'''ALTER TABLE {tab_name} RENAME COLUMN {current_col} TO {new_col};''')

    def drop_aliases(self, re_pat_list):
        
        aliases = self.tables()

        for name in aliases.index:
            for pattern in re_pat_list :

                if re.match(pattern, name) and aliases.loc[name, 'type'] == 'table' :
                    self.connect.execute ( f'''DROP TABLE if exists {name}''' )
                    print ( f'''DROP TABLE if exists {name}''')

                elif re.match(pattern, name) and aliases.loc[name, 'type'] == 'view' :
                    self.connect.execute ( f'''DROP VIEW if exists {name}''' )
                    print (f'''DROP VIEW if exists {name}''')


    def clone_aliases(self, re_pat_list : str, source_db_path : str ) :
        
        desdb   = database(source_db_path)
        aliases = desdb.tables()
        del desdb

        self.attach_database(source_db_path, 'srcdb')
        self.drop_aliases(re_pat_list)

        for name in aliases.index:
            for re_parttern in re_pat_list:
                if aliases.loc[name, 'type'] == 'table' and re.match(re_parttern, name):
                    self.connect.execute( f'''CREATE TABLE main.{name} as select * from srcdb.{name}''' )
                    print (f'''CLONE {name} from  {source_db_path}\n''')

                elif aliases.loc[name, 'type'] == 'view' and re.match(re_parttern, name):
                    self.connect.execute( aliases.loc[name, 'sql'] )
                    print (f'''CLONE view_schema {name} from {source_db_path}\n''')

        self.connect.commit()                
        self.detach_database('srcdb')
    
    def get_schema_by_table_name(self, table_name):

        self.init_spatialmetadata()

        # self.schema = {}

        df_tab_info = pandas.read_sql_query(f'''pragma table_info('{table_name}')''', self.connect, index_col=['name'])

        # self.schema[table_name] = {'cols_name': df_tab_info.index.tolist(), 
        #                             'cols_type': df_tab_info.type.tolist() }
        result = {}
        for name, type in zip(df_tab_info.index.tolist(), df_tab_info.type.tolist()):
            result[name] = type
            
        if 'geom' in df_tab_info.index:
            sql = f'''SELECT GeometryType("geom") FROM "{table_name}" group by 1''' 
            df_geom       = pandas.read_sql(sql, self.connect)
            geom = df_geom.iloc[0,0]
            result['geom'] = geom
        #     geom_info     = df_geom.iloc[ df_geom['num'].idxmax() ]
        #     self.schema[table_name]['geom'] = geom_info.drop(index='num').to_dict()

        #     if 'type' in self.schema[table_name]['geom'] :
        #         self.schema[table_name]['geom']['type_i'] = self.geom_type_dict [self.schema[table_name]['geom']['type']]

        return result