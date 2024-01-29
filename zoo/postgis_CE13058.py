import psycopg2
import pandas as pd 
import geopandas as gpd

from sqlalchemy import create_engine

class postgis_CE13058:
    def __init__(self):
        from baseobject import baseobject

        #get essential attributes from rosa_config.json
        argu = baseobject()

        #check if all essential attributes are exist in rosa_config.json
        essential_attributes = ['POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DATABASE', 'POSTGRES_USERNAME', 'POSTGRES_PASSWORD']
        for att in essential_attributes:
            if not hasattr(argu, att):
                raise AttributeError(f'{att} is not exist in rosa_config.json')

        self.host       = argu.POSTGRES_HOST
        self.database   = argu.POSTGRES_DATABASE
        self.username   = argu.POSTGRES_USERNAME
        self.password   = argu.POSTGRES_PASSWORD
 
        try:
            self.engine = create_engine(f'postgresql://{self.username}:{self.password}@{self.host}/{self.database}')
            self.conn = psycopg2.connect(f"dbname='{self.database}' user='{self.username}' host='{self.host}' password='{self.password}'")    

        except Exception as e:
            self.engine = None

    #drop table in database
    def drop_table(self, table_name):
        if self.conn is None:
            print("No connection to the database.")
            return
        try:
            with self.conn.cursor() as cursor:
                query = f"DROP TABLE {table_name};"
                cursor.execute(query)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(e)

    #read geometry table from database to geodataframe
    def read_geometry(self, table_name):
        if self.engine is None:
            print("No connection to the database.")
            return
        try:
            gdf = gpd.read_postgis(table_name, self.engine, geom_col='geom')
            return gdf
        except Exception as e:
            raise e

    #write geodataframe to database
    def write_geometry(self, geodataframe, table_name):
        if self.engine is None:
            print("No connection to the database.")
            return
        try:
            #check if table_name is exist
            query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');"
            df = pd.read_sql(query, self.engine)

            if df.iloc[0,0] == False:
                #write to database  if not exist
                geodataframe.to_postgis(table_name, self.engine, if_exists='replace', index=False)
            else:
                #alert user if table_name is exist
                raise ValueError("table_name is exist, please use append_geometry() or drop_table() first")

        except Exception as e:
            raise e

    #append dataframe to database
    def append_data(self, dataframe, table_name):
        if self.engine is None:
            print("No connection to the database.")
            return
        try:
            #check if table_name is exist
            query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');"
            df = pd.read_sql(query, self.engine)

            if df.iloc[0,0] == False:
                #write to database  if not exist
                dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
            else:
                #get columns' name in database
                query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}';"
                df = pd.read_sql(query, self.engine)
                db_columns = set(df['column_name'].values.tolist())

                #get columns' name in dataframe
                df_columns = set(dataframe.columns)
  
                columns_not_exist = df_columns - db_columns
                if len(columns_not_exist) == 0:
                    dataframe.to_sql(table_name, self.engine, if_exists='append', index=False)
                else:
                    raise ValueError(f"columns' name {columns_not_exist} is not exist in table_name {table_name}")

        except Exception as e:
            raise e

    #write dataframe to database
    def write_data(self, dataframe, table_name):

        if self.engine is None:
            print("No connection to the database.")
            return

        try:
            #check if table_name is exist
            query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');"
            df = pd.read_sql(query, self.engine)

            if df.iloc[0,0] == False:
                #write to database  if not exist
                dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
            else:
                #get columns' name in database
                query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}';"
                df = pd.read_sql(query, self.engine)
                db_columns = set(df['column_name'].values.tolist())

                #get columns' name in dataframe
                df_columns = set(dataframe.columns)
  
                columns_not_exist = df_columns - db_columns
                if len(columns_not_exist) == 0:
                    dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
                else:
                    raise ValueError(f"columns' name {columns_not_exist} is not exist in table_name {table_name}")

        except Exception as e:
            raise e
        
    #read table from database to dataframe
    def read_data(self, table_name):
        if self.engine is None:
            print("No connection to the database.")
            return
        try:
            df = pd.read_sql(table_name, self.engine)
            return df
        
        except Exception as e:
            return pd.DataFrame()

    #update dataframe to database
    def update_data(self, query):
        if self.engine is None:
            print("No connection to the database.")
            return
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                update_rows = cursor.rowcount
            
            postgis.conn.commit()

            return update_rows

        except Exception as e:
            raise e
        
    def backup(self, filename):
        #backup database to dump file
        if self.conn is None:
            print("No connection to the database.")
            return
        #plan to use pg_dump to backup database
        
        
        
        
        
        ###### implement later
        pass

    def get_entities(self):
        #get all tables's name and columns's name in database
        if self.engine is None:
            print("No connection to the database.")
            return
        try:
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            df = pd.read_sql(query, self.engine)
            tables = df['table_name'].values.tolist()
            entities = {}

            for table in tables:
                query   = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}';"

                df      = pd.read_sql(query, self.engine)
                types   = df['data_type'].values.tolist()                
                columns = df['column_name'].values.tolist()
                # make a dictionary of columns and types
                entities[table] = dict(zip(columns,types))
            return entities
        
        except Exception as e:
            print(e)

    def get_primary_key(self, table_name):
        #get primary key of table





        ##### implement later
        pass

if __name__ == '__main__':
    try:
        postgis = postgis_CE13058()

        attr = postgis.get_entities()

        for key, value in attr.items():
            print(key)
            print ('\t',value)
        geopdf = postgis.read_geometry('argu_town')
    
        # postgis.drop_table('data_rosa_ctyang_town')
        postgis.write_geometry(geopdf, 'data_rosa_fj_town_0118')

        # postgis.close()
    except Exception as e:
        print(e)