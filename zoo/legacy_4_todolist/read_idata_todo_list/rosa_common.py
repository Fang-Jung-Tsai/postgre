import os
from  lily.core import webapi
from  lily.database import manidb
from  lily.core import user_argument
from  matplotlib import pyplot as plt
import  geopandas 

class  rosa_common ():
    def __init__(self):
        self.conf = user_argument()
        self.rosa_argu = os.path.join(self.conf.root,'ROSA.common.gpkg')
        self.argu_list = ['argu_town', 'argu_tract', 'argu_county', 'argu_grid500m', 'argu_segis_A3_109']

    def get_argu(self, argu_name):

        if argu_name not in self.argu_list:
            #return empty geoDataFrame
            return geopandas.GeoDataFrame()
        else:
            ent = geopandas.read_file(self.rosa_argu, layer= argu_name) 
            return ent

if __name__ in [ '__main__' ]:
    rosa = rosa_common()
    town = rosa.get_argu('argu_county')
    town.plot()
    plt.show()


