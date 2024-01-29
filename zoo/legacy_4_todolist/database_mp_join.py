#coding=utf-8
#ctao3
# 2021May 25 version stage3
#Last modified: 20220809 by Cheng-Tao Yang


################################################################################
#####  nsg database format adapter
#####
#####

##nsg.rosa. ------. sqlite
#
#rule 1     filename prefix nsg\\.rosa\\.(.*)\\.sqlite
#
#
#rule 2  
#           ctao3.database_2
#
#rule 3 
#           table prefix
#           regular_expression_pattern = '^(schema)|(metadata)|(rawdata)|(mineral)|(argu)|(calc)|(rdset)'
#
#           nsg_key  primary key
#           geom     GIS geometry column

import re
import pandas
from ctao3.ctao3_42     import split_list_to_chunks
from ctao3.nsgstring    import alnum_uuid
from ctao3.database_2   import database_2 
from multiprocessing    import Pool

###########################################################################
##### 
#####
#####   database for  nsg_data
##### 
class database_3 (database_2):

    def __init__(self,  sourcedb_name = None):
        super().__init__(sourcedb_name) 
        self.nsg_prefix = '^(schema)|(metadata)|(rawdata)|(mineral)|(argu)|(calc)|(rdset)'

    def get_nsg_schema(self):
        schema = super().get_schema()
        sechma = {key: value for key, value in schema.items() if re.match(self.nsg_prefix, key)  }
        return sechma 

    def operation_1 (self, tab, col_expression):

        rdset_name = 'calc_op1_' + alnum_uuid()
        self.connect.execute( f'create table {rdset_name} as select {col_expression} from {tab}' )
        return rdset_name

    def operation_2 (self, a_tab, b_tab, col_expression, on_condition):

        rdset_name = f'rdset_{a_tab}_{b_tab}'    
        sql = f'create table {rdset_name} as select {col_expression} from {a_tab} left join {b_tab} on {on_condition} ''' 
        self.connect.execute(sql)
        return rdset_name

    def private_mt_2(arg):
        #just for mpool
        df_a    = arg[0]
        df_b    = arg[1]
        a_tab   = arg[2]
        b_tab   = arg[3]
        col_expression  = arg[4]
        on_condition    = arg[5]

        db = database_3()
        db.get_entity(a_tab).write(df_a)
        db.get_entity(b_tab).write(df_b)
        db.recovergeometrycolumn()
        db.connect.execute(f'''select CreateSpatialIndex('{a_tab}','geom')''')
        db.connect.execute(f'''select CreateSpatialIndex('{b_tab}','geom')''')
        rdset = db.operation_2(a_tab,b_tab,col_expression,on_condition)
        return db.get_entity(rdset).read()

    def operation_2_mt (self, a_tab, b_tab, col_expression, on_condition) :
        df_A   = self.get_entity(a_tab).read()
        df_B   = self.get_entity(b_tab).read()

        list_A = split_list_to_chunks(df_A, int(len(df_A)/20) + 1  )
        list_B = split_list_to_chunks(df_B, int(len(df_B)/20) + 1  )

        arglist     = []
        for n in range(len(list_A)):
            for m in range(len(list_B)):
                arglist.append( [list_A[n], list_B[m] , a_tab, b_tab, col_expression, on_condition] )

        #single cpu
        #
        #
        #content =[]
        #for arg in arglist:
        #    content.append( database_3.private_mt_2 (arg) )

        #result  = pandas.concat(content).drop_duplicates()
        #des_tab = f'calc_{a_tab}_{b_tab}'
        #self.get_entity(des_tab).write(result)

        #
        #
        #multi cpu
        with Pool( self.host.cpu_code  ) as mpPool:
            #計算後合併                
            content = mpPool.map(database_3.private_mt_2, arglist)
            result  = pandas.concat(content).drop_duplicates()
            #計算結果輸出
            des_tab = f'calc_{a_tab}_{b_tab}'
            self.get_entity(des_tab).write(result)


    def data_cleaning(self):
        for tab_name in self.schema :
            entity = self.get_entity(tab_name)
            df = entity.read()

            emplist = []

            for col in df.columns:
                collist = df[col].tolist()
                chset = set(collist)

                if len(chset) ==1:
                    emplist.append(col)

    def clone_data (self, re_pat_list : list, srcdb):  
        #srcdb  (instance of database_3)

        for tab_name in srcdb.entities().index:
            for re_parttern in re_pat_list:
                if re.match(re_parttern, tab_name):
                    df  = srcdb.get_entity(tab_name).read()
                    self.get_entity(tab_name).write(df)
                    print (f'CLONE data {tab_name} from {srcdb.database_path}')

        del srcdb

    #def transaction (self, SQL_list):
    #    self.connect.isolation_level = None
    #    try:
    #        for sql in SQL_list:
    #            self.cursor.execute(sql)
    #    except self.connect.Error :
    #        self.connect.rollback()

#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
###########################################################################
##### debug
#####
#####
#####  2022/08/25

if __name__ == '__console__' or __name__ == '__main__':
    #base class
    mydb = database_3 ('var/database_3.alpha_test.sqlite')

