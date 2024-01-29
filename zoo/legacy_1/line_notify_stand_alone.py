from qgis.PyQt.QtWidgets import QFileDialog
from qgis.core import (
    QgsVectorLayer, QgsProject, QgsCoordinateReferenceSystem,
    QgsVectorLayer, QgsMessageLog, Qgis, QgsProject, QgsPrintLayout, 
    QgsLayoutItemMap, QgsLayoutPoint, QgsLayoutSize, QgsUnitTypes, 
    QgsLayoutItemLabel, QgsLayoutItemLegend, QgsLayoutItemScaleBar,
    QgsLayoutExporter, QgsCoordinateReferenceSystem, QgsRasterLayer,
    QgsLayoutItemPicture, QgsApplication, QgsLayerTree)
from PyQt5.QtCore import QRectF, QDateTime
from PyQt5.QtGui import QFont, QColor
import requests
import argparse
import json
import os
import sys
sys.path.append('C:\\OSGeo4W\\apps\\qgis-ltr\\python\\plugins')
import processing
from processing.core.Processing import Processing

def export(layout, output_path, settings):
    exporter = QgsLayoutExporter(layout)
    exporter.exportToImage(output_path, settings)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', type=str, required=True)
    parser.add_argument('--output', '-o', type=str, default='default')
    parser.add_argument('--dpi', '-d', type=int, default=300)
    target_dir = parser.parse_args().input
    output_dir = parser.parse_args().output
    dpi = parser.parse_args().dpi

    QgsApplication.setPrefixPath(r'C:/OSGeo4W/apps/qgis-ltr', True)
    qgs =  QgsApplication([], False)
    qgs.initQgis()
    Processing.initialize()
    # QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

    FILES = ['Epicenter.TAB', 'Evt1Fault.TAB', 'Evt1GbsTownLoss.TAB', 
                'Evt1GmDmnd.TAB', 'ScenarioData.TAB']
    layersToAdd = []
    # TELES geometry file hardcoded for now
    plugin_dir = os.path.join(os.path.dirname(__file__), os.path.pardir)
    TOWN_FILE = os.path.join(plugin_dir, 'database', "AdmTown.TAB")
    TRACT_FILE = os.path.join(plugin_dir, 'database', "AdmTract.TAB")
    # save image to temp folder
    IMG_FOLDER = os.path.join(plugin_dir, 'database', 'images')
    # ask for target directory
    
    # target_dir = r"C:\Users\olove\Downloads\2022_0801_早期損失評估\早期評估AutoRun資料夾\D20100304\E0818"
    
    # check if all file exist
    for file in FILES:
        _path = os.path.join(target_dir, file)
        if not os.path.exists(_path):
            pass
    _time = QDateTime.currentDateTime().toString('yyyyMMdd_hhmmss')
    # get Scenario Data
    layer = QgsVectorLayer(os.path.join(target_dir, 'ScenarioData.TAB')
                            , 'ScenarioData', 'ogr')
    for feature in layer.getFeatures():
        if feature['Name'] == 'SmsDateTime2':
            ScenarioDateTime = feature['Value']
        elif feature['Name'] == 'SmsEpicLoc':
            Epicenter = feature['Value']
        elif feature['Name'] == 'SmsRespLevel':
            RespLevel = feature['Value']
        elif feature['Name'] == 'SmsEpicX':
                EpicX = feature['Value']
        elif feature['Name'] == 'SmsEpicY':
            EpicY = feature['Value']
        elif feature['Name'] == 'SmsDepth':
            EpicDepth = feature['Value']
        elif feature['Name'] == 'SmsMag':
            EpicMag = feature['Value']
    ## change to OpenStreetMap as base map
    # # load county layer as base map
    # _layer = QgsVectorLayer(COUNTY_FILE, 'county')
    # _layer.setCrs(QgsCoordinateReferenceSystem('EPSG:3826'))
    # QgsProject.instance().addMapLayer(_layer)
    # _layer.loadNamedStyle(
    #     os.path.join(plugin_dir, 'qml', 'line_notify', 'teles_county.qml'))
    # load OpenStreetMap as base map
    tms = 'type=xyz&url=https://tile.openstreetmap.org/{z}/{x}/{y}.png&zmax=19&zmin=0'
    _layer = QgsRasterLayer(tms, 'osm', 'wms')
    QgsProject.instance().addMapLayer(_layer)
    # get tract ground motion layer by joining table and geometry
    # tract_layer: tract geometry
    tract_layer = QgsVectorLayer(TRACT_FILE, 'tract_layer')
    tract_layer.setCrs(QgsCoordinateReferenceSystem('EPSG:3826'))
    _path = os.path.join(target_dir, 'evt1GmDmnd.TAB')
    # # tract_table: tract ground motion table
    # tract_table = QgsVectorLayer(_path, 'evt1GmDmnd_table')
    # # join tract_table to tract_layer with processing
    # _parameter = {'INPUT': tract_layer, 'FIELD': 'Tract',
    #               'INPUT_2': tract_table, 'FIELD_2': 'Tract','METHOD': 0,
    #               'OUTPUT': 'memory:', 'DISCARD_NONMATCHING': True}
    # output_tract = processing.run(
    #     'qgis:joinattributestable', parameters=_parameter)['OUTPUT']
    # output_tract.setName('Ground Motion (tract)')
    # QgsProject.instance().addMapLayer(output_tract)
    # output_tract.loadNamedStyle(
    #     os.path.join(plugin_dir, 'qml', 'line_notify', 'teles_tract.qml'))
    # output_tract.setCrs(QgsCoordinateReferenceSystem('EPSG:3826'))
    # get town loss layer by joining table and geometry
    # town_layer: town geometry
    town_layer = QgsVectorLayer(TOWN_FILE, 'town_layer')
    town_layer.setCrs(QgsCoordinateReferenceSystem('EPSG:3826'))
    _path = os.path.join(target_dir, 'Evt1GbsTownLoss.TAB')
    # town_table: town loss table
    town_table = QgsVectorLayer(_path, 'TownLoss_table')
    # join town_table to town_layer with processing
    _parameter = {'INPUT': town_layer, 'FIELD': 'Town_code',
                    'INPUT_2': town_table, 'FIELD_2': 'Town_code','METHOD': 0,
                    'OUTPUT': 'memory:', 'DISCARD_NONMATCHING': True}
    output_town = processing.run(
        'qgis:joinattributestable', parameters=_parameter)['OUTPUT']
    output_town.setName('推估損失 (鄉鎮市區)')
    output_town.setCrs(QgsCoordinateReferenceSystem('EPSG:3826'))
    _l = QgsProject.instance().addMapLayer(output_town)
    layersToAdd.append(_l)
    output_town.loadNamedStyle(
        os.path.join(plugin_dir, 'qml', 'line_notify', 'teles_town.qml'))

    # load epicenter layer and fault layer
    _path = os.path.join(target_dir, 'Epicenter.TAB')
    _layer = QgsVectorLayer(_path, '震央', 'ogr')
    _layer.setCrs(QgsCoordinateReferenceSystem('EPSG:4326'))
    _l = QgsProject.instance().addMapLayer(_layer)
    layersToAdd.append(_l)
    _layer.loadNamedStyle(
        os.path.join(plugin_dir, 'qml', 'line_notify', 'Epicenter.qml'))
    _path = os.path.join(target_dir, 'Evt1Fault.TAB')
    _layer = QgsVectorLayer(_path, '斷層', 'ogr')
    _layer.setCrs(QgsCoordinateReferenceSystem('EPSG:4326'))
    _l = QgsProject.instance().addMapLayer(_layer)
    layersToAdd.append(_l)
    _layer.loadNamedStyle(
        os.path.join(plugin_dir, 'qml', 'line_notify', 'Fault.qml'))




    # load layout
    layout = QgsPrintLayout(QgsProject.instance())
    layout.initializeDefaults()
    layout.setName(
        f"Instant Capture-{_time}")
    QgsProject.instance().layoutManager().addLayout(layout)
    # add map to layout
    map = QgsLayoutItemMap(layout)
    map.setRect(QRectF(0, 0, 200, 100))
    # Set map item position and size 
    # (by default, it is a 0 width/0 height item placed at 0,0)
    map.attemptMove(QgsLayoutPoint(10, 30, QgsUnitTypes.LayoutMillimeters))
    map.attemptResize(
        QgsLayoutSize(277,170, QgsUnitTypes.LayoutMillimeters))
    # Provide an extent to render
    map.setCrs(QgsCoordinateReferenceSystem('EPSG:3826'))
    map.zoomToExtent(output_town.extent())
    map.setFrameEnabled(True)
    layout.addLayoutItem(map)
    # add title
    label = QgsLayoutItemLabel(layout)
    _text = f'時間 : {ScenarioDateTime}\
            \n震央 : {Epicenter}\
            \n警戒階段 : {RespLevel}'
    label.setText(_text)
    font = QFont()
    font.setPointSize(16)
    label.setFont(font)
    label.setFixedSize(
        QgsLayoutSize(80, 20, QgsUnitTypes.LayoutMillimeters))
    label.setBackgroundColor(QColor(255, 255, 255))
    label.setBackgroundEnabled(True)
    label.setFrameEnabled(True)
    label.attemptMove(
        QgsLayoutPoint(10, 5, QgsUnitTypes.LayoutMillimeters))
    layout.addLayoutItem(label)
    # add title
    label2 = QgsLayoutItemLabel(layout)
    _text = f'震源深度 : {EpicDepth}km\
            \n震央經緯座標 : {EpicX}°E, {EpicY}°N\
            \n地震規模 : {EpicMag}'
    label2.setText(_text)
    font = QFont()
    font.setPointSize(16)
    label2.setFont(font)
    label2.setFixedSize(
        QgsLayoutSize(80, 20, QgsUnitTypes.LayoutMillimeters))
    label2.setBackgroundColor(QColor(255, 255, 255))
    label2.setBackgroundEnabled(True)
    label2.setFrameEnabled(True)
    label2.attemptMove(
        QgsLayoutPoint(90, 5, QgsUnitTypes.LayoutMillimeters))
    layout.addLayoutItem(label2)
    # add legend
    legend = QgsLayoutItemLegend(layout)
    legend.setTitle('圖例')
    root = QgsLayerTree()
    for layer in layersToAdd:
        #add layer objects to the layer tree
        root.addLayer(layer)
    legend.model().setRootGroup(root)
    legend.setLinkedMap(map) # map is an instance of QgsLayoutItemMap
    legend.attemptMove(
        QgsLayoutPoint(225,140, QgsUnitTypes.LayoutMillimeters))
    legend.setFrameEnabled(True)
    legend.setAutoUpdateModel(False)
    # remove osm from legend
    root = legend.model().rootGroup()
    root.removeChildNode(root.children()[-1])
    legend.resizeToContents()
    layout.addLayoutItem(legend)
    # add scale bar
    scaleBar = QgsLayoutItemScaleBar(layout)
    scaleBar.setLinkedMap(map)
    scaleBar.setStyle('Single Box')
    scaleBar.applyDefaultSize()
    scaleBar.setNumberOfSegments(3)
    scaleBar.setReferencePoint(QgsLayoutItemScaleBar.UpperRight)
    scaleBar.attemptMove(
        QgsLayoutPoint(287, 10, QgsUnitTypes.LayoutMillimeters))
    scaleBar.setFrameEnabled(True)
    scaleBar.setBackgroundColor(QColor(255, 255, 255))
    scaleBar.setUnitLabel('km')
    scaleBar.setUnits(QgsUnitTypes.DistanceKilometers)
    scaleBar.setUnitsPerSegment(10)
    scaleBar.setBackgroundEnabled(True)
    layout.addLayoutItem(scaleBar)
    # add image
    image = QgsLayoutItemPicture(layout)
    image.setPicturePath(
        os.path.join(plugin_dir, 'icon', 'ncree.png'))
    image.setOpacity(0.1)
    image.attemptMove(
        QgsLayoutPoint(10, 10, QgsUnitTypes.LayoutMillimeters))
    image.setFixedSize(
        QgsLayoutSize(277, 190, QgsUnitTypes.LayoutMillimeters))
    image.setPictureRotation(30)
    layout.addLayoutItem(image)
    # export layout
    # exporter = QgsLayoutExporter(layout)
    if output_dir == 'default':
        _path = os.path.join(IMG_FOLDER, f"{_time}.jpg")
    else:
        _path = output_dir
    _setting = QgsLayoutExporter.ImageExportSettings()
    # let user decide dpi in the future
    _setting.dpi = dpi
    export(layout, _path, _setting)
    _file = open(_path, 'rb')
    IMGURL = 'https://api.imgur.com/3/image'
    param = {"client_id": "f6b3fe1c2301deb"}
    files = {"image": _file,
                "type": "file"}
    r = requests.post(IMGURL, params=param, files=files)
    link = r.json()['data']['link']
    URL = 'https://api.line.me/v2/bot/message/multicast'
    TOKEN = 'W5FwqZw+TnefbIGyBaqZF5zK0SMWfiHG0Iyo425sgrbrOIviwrUm+gEzlEA760r53CPzSLY0VDtXlriIkHFdyzPy+Izpx3rNVvYKk6SKX3LlyskT3Se8t0WvsTpTk4WUTHE0IJ/s9EmKcHMDnotHfwdB04t89/1O/w1cDnyilFU='
    headers = {"Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json"}
    recivers = ['U8b0e20516d78c4b4af2d4e1fb807c166']
    data = {"to": recivers,
            "messages": [{"type": "image",
                            "originalContentUrl": link,
                            "previewImageUrl": link}]}
    requests.post(URL, headers=headers, data=json.dumps(data))
    _file.close()

    qgs.exitQgis()
    print('done ', os.path.realpath(_path))