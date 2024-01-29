###########################################
# just   for ctao2_release
class ctao2_digraph:
    __file__ = __file__

###########################################

class graph:
    def __init__(self, dataframe_edge, dataframe_node):
        import pandas
        import networkx as nx

        self.DiGraph        = nx.DiGraph()
        self.dataframe_edge = dataframe_edge
        self.dataframe_node = dataframe_node

        for key, row in dataframe_node.iterrows():
            node    = row['fnsg_08x']
            supply  = row['val']
            self.DiGraph.add_node(node, val=supply  )

        counter = 0
        for key, row in dataframe_edge.iterrows():
            self.DiGraph.add_edge( key[0], key[1], cost = row['vt_cost'], capacity = row['width']*2000)
            counter = counter + 1

        print ('graph __init__ edge',counter)

    def capacity_scaling(self):

        import networkx as nx
        self.dataframe_edge['flow'] = 0

        self.cost, self.flow = nx.capacity_scaling(self.DiGraph, demand = 'val',  capacity = 'capacity',  weight='cost')

        for fnode, dli in self.flow.items():
            for tnode, edge_flow in dli.items():
                if edge_flow !=0 :
                    self.dataframe_edge.loc[(fnode, tnode), 'flow'] = edge_flow

    def get_nzeroflow(self):
        #To select rows whose column value equals a scalar, some_value, use ==:
        #df.loc[df['column_name'] == some_value]

        #To select rows whose column value is in an iterable, some_values, use isin:
        #df.loc[df['column_name'].isin(some_values)]
        return self.dataframe_edge.loc [ self.dataframe_edge['flow'] > 0 ]

    def clear_node_val(self):
        import networkx as nx
        nx.set_node_attributes(self.DiGraph, [0], 'val')

    def get_node_val(self):
        import networkx as nx
        node = nx.get_node_attributes(self.DiGraph, 'val')
        return node

    def set_node_val(self, node):
        import networkx as nx
        node = nx.set_node_attributes(self.DiGraph, node, 'val')
