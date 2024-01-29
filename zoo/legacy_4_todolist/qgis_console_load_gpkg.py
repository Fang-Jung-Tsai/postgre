from qgis.PyQt import *
from qgis.PyQt import uic, QtWidgets
from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtCore import *
from qgis.PyQt.QtWidgets import *
from qgis.core import *
from qgis.gui import *
import sys
from qgis.utils import iface

from qgis.core import( 
    QgsProject, 
    QgsCoordinateReferenceSystem, 
    QgsCoordinateTransform, 
    QgsLayoutExporter, 
    QgsApplication, 
    QgsGeometry)


class load_gpkg():

    def __init__(self):
        self.Rosa = None
        self.project_name = None
        self.project_db = None
        self.argu_road = None
        self.argu_town = None

    def load_gpkg(path):
            """
            開啟時載入圖層
            """
            msg = QMessageBox()
            msg.setWindowTitle('Error')
            msg.setIcon(QMessageBox.Warning)
            msg.setText("Please select town and road layers from config first.\nOpen config window?")
            msg.setStandardButtons(QMessageBox.Yes|QMessageBox.Cancel)

            reply = msg.exec_()

            if reply == QMessageBox.Yes:
                 print ('Yes clicked.')

            # self.project_name = gl.get_value('project_name')
            # if self.project_name == None:
            #     self.close()
            #     reply = msg.exec_()
            #     if reply == QMessageBox.Yes:
            #         self.Rosa.config()
            #     return()
            # else:
            #     path = os.path.join(self.Rosa.plugin_dir, 'database', 'project', self.project_name)
            #     #self.project_db = manidb(path)
            #     tables = ['argu_town', 'argu_road']
            #     uri = QgsDataSourceUri()
            #     uri.setDatabase(path)
            #     schema = ''
            #     geom_column = 'geom'

            #     for table in tables:
            #         layer = f'self.{table}'
            #         uri.setDataSource(schema, table, geom_column)
            #         exec(f"{layer} = self.Rosa.iface.addVectorLayer(uri.uri(), table,'spatialite')")
            #         layer = eval(layer)
            #         path = os.path.join(self.Rosa.plugin_dir, 'qml', f'{table}.qml')
            #         layer.loadNamedStyle(path)
            #         layer.triggerRepaint()
            #     self.Rosa.iface.actionSelect().trigger()
            # self.argu_road.selectionChanged.connect(self.select_to_table)

            # self.argu_town.setSubsetString(""""cnty_name" IN ('台北市','新北市')""")

            # layer = self.argu_road
            # self.cache = QgsVectorLayerCache(layer, layer.featureCount())

            # if not 'priority' in layer.fields().names():
            #     pr = layer.dataProvider()
            #     priority = QgsField('priority', QVariant.String)
            #     with edit(layer):
            #         pr.addAttributes([priority])

            # self.load_roadcode()

if __name__ == '__console__' or __name__ == '__main__':
    import sys

    obj = load_gpkg()
    obj.load_gpkg()
