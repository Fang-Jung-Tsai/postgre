# 2021May 25 version stage3
import re
import os
import json
import pandas
from datetime import datetime
from multiprocessing        import Pool
from multiprocessing        import Process
from Lily.ctao2.ctao2_42    import answer   
from Lily.ctao2.ctao2_42    import split_list_to_chunks
from Lily.ctao2.ctao2_database_alias import manidb

class database_cross_mt(manidb):
          
    def unary_operation (arg):
        col_expression  = arg[0]
        df_A            = arg[1]   
        
        memodb = manidb(':memory:')
        memodb.get_alias('A').write(df_A)
        
        memodb.connect.execute( f'''create table rdset as select A.* , {col_expression} from A ''' )

        return memodb.read_sql( f'''select * from rdset''')

    def binary_operation (arg):

        geom_fun  = arg[0][0]
        geom_con  = arg[0][1]
        right_tab = arg[0][2]

        memodb = manidb(':memory:')
        memodb.get_alias('A').write(arg[1])                 #df_A            = arg[1] 
        memodb.get_alias('B').write(arg[2])                 #df_B            = arg[2]

        sql =   f'''create table leftj as select {geom_fun}(A.geom, B.geom) as geom_calc, B.nsg_key as {right_tab}_key, A.* 
                from A left join B on within(A.geom, B.geom)  {geom_con} ''' 

        memodb.connect.execute(sql)

        return memodb.read_sql(f'''select * from leftj where {right_tab}_key is not null''')

#######################

    def __init__(self, sourcedb_filename):
        super().__init__(sourcedb_filename)
        self.uAnswer = answer()

    def left_join_on_geom_fun(self, left_table : str, right_table : str, spatialite_geom_fun : str, geom_condition : str) :

        self.df_A   = self.get_alias(left_table).read()
        self.df_B   = self.get_alias(right_table).read()

        if not set(['nsg_key','geom']).issubset(self.df_A.columns) \
            or not set(['nsg_key','geom']).issubset(self.df_B.columns):
            print ('Table without nsg_key or geom columns')
            return

        self.list_A = split_list_to_chunks(self.df_A, int(len(self.df_A)/10) + 1  )
        self.list_B = split_list_to_chunks(self.df_B, int(len(self.df_B)/10) + 1  )

        arglist     = []
        for n in range(len(self.list_A)):
            for m in range(len(self.list_B)):
                arglist.append( ([spatialite_geom_fun, geom_condition, right_table], self.list_A[n], self.list_B[m] ) )

        with Pool( self.uAnswer.host.cpu_code  ) as mpPool:
            #多cpu 計算後合併
                                 
            content = mpPool.map(database_cross_mt.binary_operation, arglist)
            result  = pandas.concat(content).drop_duplicates()
            #計算結果輸出
            des_tab = left_table  +  '_lj_' + right_table
            self.get_alias(des_tab).write(result)

    def transform(self, src_table : str, col_expression : str) :

        self.df_A   = self.get_alias(src_table).read()
        self.list_A = split_list_to_chunks(self.df_A, int(len(self.df_A)/1000) + 1  )

        arglist     = []
        for n in range(len(self.list_A)):
            arglist.append( (col_expression, self.list_A[n] ))

        with Pool(self.uAnswer.host.cpu_code ) as mpPool:
            content = mpPool.map(database_cross_mt.unary_operation, arglist)
            result  = pandas.concat(content).drop_duplicates()

            des_tab = src_table  +  '_cal' 
            self.get_alias(des_tab).write( result )


    def left_outer_join(self,  args):
        '''測試中功能 '''
        #arg_dict = {   
        #    'tableA'        : 'left table',
        #    'tableB'        : 'right table',
        #    'keyA'          : ['nsg_key'],
        #    'keyB'          : ['nsg_key'],
        #    'colsA'         : ['rd4','rd5'],
        #    'colsB'         : ['Rd4','Level'],
        #    'table_target'  : 'argu_graph_road_inktty' 
        # }

        part_A  = self.get_alias(args['tableA']).read()
        part_B  = self.get_alias(args['tableB']).read() 

        part_B = part_B[ args[colsB] ]
  
        part_A.merge(part_B, how='left', left_on = args['keyA'], right_on = Ture)

        part_A.to_sql(args['table_target'], self.connect, if_exists='replace', index=True) 