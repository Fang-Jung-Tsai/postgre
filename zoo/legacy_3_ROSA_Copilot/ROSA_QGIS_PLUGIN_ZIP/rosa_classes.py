import abc
import os
import warnings
from functools import singledispatchmethod
from qgis.core import QgsProject, QgsVectorLayer, QgsDataSourceUri

class CanvasManager:

    def __init__(self, ROSA):
        self.Rosa = ROSA
        self.layers = []

    @singledispatchmethod
    def add_layer(self, layer, name = None, qml = None):
        # check if layer is already in the list
        for l in self.layers:
            if l['id'] == layer.id():
                return
        name = layer.name()
        _source = layer.source()
        db = _source[8:_source.find('table=') - 2]
        table = _source[_source.find('table=') + 7:_source.find('" (geom)')]
        _id = layer.id()

        layer_dict = {'name': name, 'db': db, 'table': table, 'vectorLayer': layer
                      , 'id': _id}
        # add layer to the list
        self.layers.append(layer_dict)
        # add Layer to Canvas
        QgsProject.instance().addMapLayer(layer)
        return layer

    @add_layer.register
    def _(self, layer: str, name = None, qml = None):
        # check string type
        if layer.endswith('.shp'):
            # get QgsVectorLayer from the string 
            layer_vector = QgsVectorLayer(layer, '', 'ogr')
            self.add_layer(layer_vector)
            QgsProject.instance().addMapLayer(layer_vector)
            return layer_vector
        else:
            raise TypeError('passing a string that is not a shapefile')

    @add_layer.register
    def _(self, layer: tuple, name = None, qml = None):
        """(sqlite path, table)"""
        # check tuple type
        if len(layer) == 2:
            # get QgsVectorLayer from the string 
            uri = QgsDataSourceUri()
            uri.setDatabase(layer[0])
            uri.setDataSource('', layer[1], 'geom')
            if name:
                layer_vector = QgsVectorLayer(uri.uri(), name, 'spatialite')
            else:
                layer_vector = QgsVectorLayer(uri.uri(), layer[1], 'spatialite')
            _layer = self.add_layer(layer_vector)
            if qml:
                path = os.path.join(self.Rosa.plugin_dir, 'qml', 'argu_shelter.qml')
                _layer.loadNamedStyle(path)
                _layer.triggerRepaint()
            return _layer
        else:
            raise TypeError('(sqlite, table)')


    @singledispatchmethod
    def set_visibility(self, layer:QgsVectorLayer, visible:bool):
        """return 1 if not found"""
        # change the layer visibility
        _node = QgsProject.instance().layerTreeRoot().findLayer(layer)
        if _node:
            _node.setItemVisibilityChecked(visible)
            return 0
        else:
            warnings.warn('rosa.CanvasManager: layer not found')
            return 1

    @set_visibility.register
    def _(self, layer:str, visible:bool):
        # find layer by name or id
        # id
        node_ = QgsProject.instance().layerTreeRoot().findLayer(layer)
        if node_:
            node_.setItemVisibilityChecked(visible)
            return 0
        else:
            # name
            _layer = QgsProject.instance().mapLayersByName(layer)
            if len(_layer) == 1:
                self.set_visibility(_layer[0], visible)
                return 0
            elif len(_layer) > 1:
                self.set_visibility(_layer[0], visible)
                warnings.warn('rosa.CanvasManager: more than one layer found')
                return 0
            else:
                warnings.warn('rosa.CanvasManager: layer not found')
                return 1

    @singledispatchmethod
    def remove_layer(self, layer:QgsVectorLayer):
        # remove layer from the list
        _removed = False
        for l in self.layers:
            if l['id'] == layer.id():
                self.layers.remove(l)
                _removed = True
                break
        if not _removed:
            warnings.warn('layer not found in CanvasManager')
            return 1
        # remove layer from the canvas
        try:
            QgsProject.instance().removeMapLayer(layer)
            self.Rosa.iface.mapCanvas().refresh()
            return 0
        except Exception as e:
            warnings.warn(e)
            return 1

    @remove_layer.register
    def _(self, layer:str):
        # find layer by name
        _layer = QgsProject.instance().mapLayersByName(layer)
        if len(_layer) == 1:
            self.remove_layer(_layer[0])
            return 0
        elif len(_layer) > 1:
            self.remove_layer(_layer[0])
            warnings.warn('rosa.CanvasManager: more than one layer found')
            return 0
        else:
            warnings.warn('rosa.CanvasManager: layer not found')
            return 1

    def remove_all_layers(self):
        # remove all layers from the list
        for layer in self.layers:
            self.remove_layer(layer['vectorLayer'])
        self.layers = []
        return 0

    def find_layer(self, layer:str)->QgsVectorLayer:
        _layer = QgsProject.instance().mapLayersByName(layer)
        if len(_layer) == 1:
            return _layer[0]
        elif len(_layer) > 1:
            warnings.warn('rosa.CanvasManager: more than one layer found')
            return _layer[0]
        else:
            warnings.warn('rosa.CanvasManager: no layer found')
            return None

class RosaFunction(abc.ABC):

    @abc.abstractmethod
    def __init__(self):
        # self.Rosa = Rosa
        # self.dialog = dialog
        return NotImplemented

    @abc.abstractmethod
    def main(self, Rosa):
        # self.dialog.show()
        return NotImplemented

    @abc.abstractmethod
    def close(self):
        # self.dialog.close()
        return NotImplemented