from qgis.PyQt.QtWidgets import QFileDialog
from qgis.core import (
    QgsVectorLayer, QgsProject, QgsCoordinateReferenceSystem,
    QgsVectorLayer, QgsMessageLog, Qgis, QgsProject, QgsPrintLayout, 
    QgsLayoutItemMap, QgsLayoutPoint, QgsLayoutSize, QgsUnitTypes, 
    QgsLayoutItemLabel, QgsLayoutItemLegend, QgsLayoutItemScaleBar,
    QgsLayoutExporter, QgsCoordinateReferenceSystem, QgsRasterLayer,
    QgsLayoutItemPicture)
from PyQt5.QtCore import QRectF, QDateTime
from PyQt5.QtGui import QFont, QColor
import processing
import requests
import json
import os
import sys
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))
import globalvar as gl
from rosa_classes import RosaFunction
import boto3
import shutil

with open(os.path.join(os.path.dirname(__file__),'keys.json')) as f:
    keys = json.load(f)
AWS_ACCESS_KEY = keys['AWS_ACCESS_KEY']
AWS_SECERT_KEY = keys['AWS_SECERT_KEY']
TOKEN = keys['TOKEN']
# ROSA_L_GROUP1 = eval(keys['ROSA_L_GROUP1'])
# ROSA_L_GROUP2 = eval(keys['ROSA_L_GROUP2'])
BUCKET = keys['IMAGE_BUCKET']
REGION = keys['REGION']
PLUGIN_DIR = os.path.join(os.path.dirname(__file__), os.path.pardir)
IMG_FOLDER = os.path.join(PLUGIN_DIR, 'database', 'images')

class LineNotify(RosaFunction):

    FILES = ['Epicenter.DBF', 'Epicenter.ID', 'Epicenter.MAP', 'Epicenter.TAB',
        'Evt1GbsTownLoss.TAB', 'Evt1GbsTownLoss.IND', 'Evt1GbsTownLoss.DBF',
        'ScenarioData.TAB', 'ScenarioData.DBF', 'Evt1GmDmnd.TAB', 'Evt1GmDmnd.DBF']
    FILES_NO_CAS = ['Epicenter.DBF', 'Epicenter.ID', 'Epicenter.MAP', 
        'Epicenter.TAB', 'ScenarioData.TAB', 'ScenarioData.DBF']
    WORKING_DIR = os.path.join(os.path.dirname(__file__), 'line_notify_working_dir')

    def __init__(self):
        self.Rosa = gl.get_value('Rosa')
        self.qml_dir = os.path.join(self.Rosa.plugin_dir, 'qml', 'line_notify')
    
    def main(self):
        target_dir = QFileDialog.getExistingDirectory(
            self.Rosa.iface.mainWindow(), 'Select TELES Folder')
        if not target_dir:
            return
        NO_CAS = True
        # copy all files to working dir
        for file in os.listdir(self.WORKING_DIR):
            if file in self.FILES:
                os.remove(os.path.join(self.WORKING_DIR, file))
                if file == 'Evt1GbsTownLoss.TAB':
                    NO_CAS = False
        if NO_CAS != True:
            for file in self.FILES:
                _path = os.path.join(target_dir, file)
                # move file to project folder
                _new_path = os.path.join(self.WORKING_DIR, file)
                # check if file exists
                if os.path.exists(_path):
                    shutil.copy(_path, _new_path)

        else:
            for file in self.FILES_NO_CAS:
                _path = os.path.join(target_dir, file)
                # move file to project folder
                _new_path = os.path.join(self.WORKING_DIR, file)
                # check if file exists
                if os.path.exists(_path):
                    shutil.copy(_path, _new_path)
        # open project
        if 'Evt1GbsTownLoss.TAB' in os.listdir(os.path.join(os.path.dirname(__file__), 'line_notify_working_dir')):
            QgsProject.instance().read(os.path.join(self.WORKING_DIR, 'project.qgz'))
        else:
            QgsProject.instance().read(os.path.join(self.WORKING_DIR, 'project_no_cas.qgz'))
        self.Rosa.output_filename = 'line_notify.jpg'
        self.export_image()
        # self.send()

    def export_image(self):
        # export layout
        layout = QgsProject.instance().layoutManager().layoutByName('rosa_line_notify')
        self.output_path = os.path.join(IMG_FOLDER, self.Rosa.output_filename)
        setting = QgsLayoutExporter.ImageExportSettings()
        setting.dpi = 300
        exporter = QgsLayoutExporter(layout)
        result = exporter.exportToImage(self.output_path, setting)
        if result == QgsLayoutExporter.Success:
            QgsMessageLog.logMessage(
                f"Output success! <a href=\"file:///{self.output_path}\">{self.output_path}</a>", 
                tag='ROSA', level=Qgis.Info)

        setting = QgsLayoutExporter.ImageExportSettings()
        setting.dpi = 48
        _path = self.output_path[:-4] + "_s.jpg"
        result = exporter.exportToImage(_path, setting)
        if result == QgsLayoutExporter.Success:
            QgsMessageLog.logMessage(
                f"Output success! <a href=\"file:///{_path}\">{_path}</a>", 
                tag='ROSA', level=Qgis.Info)

    def send(self):
        # check user list
        user_csv_path = os.path.join(self.Rosa.plugin_dir, 'ui', 'line_notify_working_dir', 'users_groups.csv')
        df = pd.read_csv(user_csv_path)
        receivers_df = df.loc[df['group'] == 1, ['name', 'user_id']]
        QgsMessageLog.logMessage(
            f"sending to {receivers_df['name'].to_list()}", tag='ROSA', level=Qgis.Info)
        receivers = receivers_df['user_id'].to_list()

        # handle group id
        receivers_group = []
        for i, r in enumerate(receivers):
            if r.startswith('C'):
                receivers.pop(i)
                receivers_group.append(r)

        link, link_pre = self.upload_to_s3()
        URL = 'https://api.line.me/v2/bot/message/multicast'
        headers = {"Authorization": f"Bearer {TOKEN}",
                    "Content-Type": "application/json"}
        data = {"to": receivers,
                "messages": [{"type": "image",
                                "originalContentUrl": link,
                                "previewImageUrl": link_pre}]}
        requests.post(URL, headers=headers, data=json.dumps(data))
        
        # send to group
        URL = 'https://api.line.me/v2/bot/message/push'
        for g in receivers_group:
            data = {"to": g,
                    "messages": [{"type": "image",
                                "originalContentUrl": link,
                                "previewImageUrl": link_pre}]}
            requests.post(URL, headers=headers, data=json.dumps(data))
    
    def upload_to_s3(self):
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECERT_KEY)
        _file = open(self.output_path, 'rb')
        _filename = QDateTime.currentDateTime().toString('yyyyMMddhhmm') + os.path.basename(self.output_path)
        s3.upload_fileobj(_file, BUCKET, _filename)
        
        _file2 = open(self.output_path[:-4] + "_s.jpg", 'rb')
        _filename2 = QDateTime.currentDateTime().toString('yyyyMMddhhmm') + os.path.basename(self.output_path[:-4] + "_s.jpg")
        s3.upload_fileobj(_file2, BUCKET, _filename2)
        
        return f"https://{BUCKET}.s3-{REGION}.amazonaws.com/{_filename}", f"https://{BUCKET}.s3-{REGION}.amazonaws.com/{_filename2}"
    
    
    def _main(self):
        # TELES geometry file hardcoded for now
        TOWN_FILE = os.path.join(self.Rosa.plugin_dir, 'database', "AdmTown.TAB")
        TRACT_FILE = os.path.join(self.Rosa.plugin_dir, 'database', "AdmTract.TAB")
        # save image to temp folder
        IMG_FOLDER = os.path.join(self.Rosa.plugin_dir, 'database', 'images')
        # ask for target directory
        target_dir = QFileDialog.getExistingDirectory(
            self.Rosa.iface.mainWindow(), 'Select TELES Folder')
        # check if all file exist
        for file in self.FILES:
            _path = os.path.join(target_dir, file)
            if not os.path.exists(_path):
                self.Rosa.iface.messageBar().pushCritical(
                    'Error', f'File {_path} does not exist')
                return
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
        #     os.path.join(self.Rosa.plugin_dir, 'qml', 'teles_county.qml'))
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
        #     os.path.join(self.Rosa.plugin_dir, 'qml', 'teles_tract.qml'))
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
        QgsProject.instance().addMapLayer(output_town)
        output_town.loadNamedStyle(
            os.path.join(self.qml_dir, 'teles_town.qml'))

        # load epicenter layer and fault layer
        _path = os.path.join(target_dir, 'Epicenter.TAB')
        _layer = QgsVectorLayer(_path, '震央', 'ogr')
        _layer.setCrs(QgsCoordinateReferenceSystem('EPSG:4326'))
        QgsProject.instance().addMapLayer(_layer)
        _layer.loadNamedStyle(
            os.path.join(self.qml_dir, 'Epicenter.qml'))
        _path = os.path.join(target_dir, 'Evt1Fault.TAB')
        _layer = QgsVectorLayer(_path, '斷層', 'ogr')
        _layer.setCrs(QgsCoordinateReferenceSystem('EPSG:4326'))
        QgsProject.instance().addMapLayer(_layer)
        _layer.loadNamedStyle(
            os.path.join(self.qml_dir, 'Fault.qml'))


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
            os.path.join(self.Rosa.plugin_dir, 'icon', 'ncree.png'))
        image.setOpacity(0.1)
        image.attemptMove(
            QgsLayoutPoint(10, 10, QgsUnitTypes.LayoutMillimeters))
        image.setFixedSize(
            QgsLayoutSize(277, 190, QgsUnitTypes.LayoutMillimeters))
        image.setPictureRotation(30)
        layout.addLayoutItem(image)
        # export layout
        exporter = QgsLayoutExporter(layout)
        _path = os.path.join(IMG_FOLDER, f"{_time}.png")
        _setting = QgsLayoutExporter.ImageExportSettings()
        # let user decide dpi in the future
        _setting.dpi = 300
        # show layout
        self.Rosa.iface.openLayoutDesigner(layout)
        # export layout to image
        exporter.exportToImage(_path, _setting)
        # inform user
        QgsMessageLog.logMessage(
            f"<b>Instant Capture</b>: Canvas View Saved to \
              <a href=\"file:///{_path}\">{_path}</a>",
            "Instant Capture", Qgis.Info)
        self.Rosa.iface.messageBar().pushSuccess(
            '', f"<b>Instant Capture</b>: Canvas View Saved to \
                  <a href=\"file:///{_path}\">{_path}</a>")
        # push line notify
        # TOKEN2 = 'fuj2ROVrpy9cevHWRUe7VwCyxQUfszbIbjjOqtjtr3T'
        # _file = open(_path, 'rb')
        # _time = QDateTime.currentDateTime().toString('yyyy/MM/dd hh:mm:ss')
        # self.Rosa.r = requests.post(
        #     f"https://notify-api.line.me/api/notify",
        #     headers={"Authorization": f"Bearer {TOKEN2}"},
        #     data={"message": f"Snap shot from QGIS by `{os.getlogin()}`\
        #         \n{_time}\n _by QGIS Plugin Instant Capture_"},
        #     files={"imageFile": _file})
        # _file.close()
        # line bot push
        # _file = open(_path, 'rb')
        # IMGURL = 'https://api.imgur.com/3/image'
        # param = {"client_id": "f6b3fe1c2301deb"}
        # files = {"image": _file,
        #          "type": "file"}
        # r = requests.post(IMGURL, params=param, files=files)
        # link = r.json()['data']['link']
        # URL = 'https://api.line.me/v2/bot/message/multicast'
        # TOKEN = 'W5FwqZw+TnefbIGyBaqZF5zK0SMWfiHG0Iyo425sgrbrOIviwrUm+gEzlEA760r53CPzSLY0VDtXlriIkHFdyzPy+Izpx3rNVvYKk6SKX3LlyskT3Se8t0WvsTpTk4WUTHE0IJ/s9EmKcHMDnotHfwdB04t89/1O/w1cDnyilFU='
        # headers = {"Authorization": f"Bearer {TOKEN}",
        #             "Content-Type": "application/json"}
        # recivers = ['U8b0e20516d78c4b4af2d4e1fb807c166']
        # data = {"to": recivers,
        #         "messages": [{"type": "image",
        #                       "originalContentUrl": link,
        #                       "previewImageUrl": link}]}
        # requests.post(URL, headers=headers, data=json.dumps(data))
        # _file.close()
        return True
        r = requests.post(URL, headers=headers, data=data, files=files)
        _file.close()
        self.Rosa.iface.mapCanvas().setExtent(output_town.extent())
        QgsProject.instance().setCrs(QgsCoordinateReferenceSystem(3826))

    def close(self):
        pass
