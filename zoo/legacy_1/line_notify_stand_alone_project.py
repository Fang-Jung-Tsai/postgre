import argparse
import os
import shutil
from PyQt5.QtCore import QDateTime
from qgis.core import (
    QgsProject, QgsCoordinateReferenceSystem, QgsProject, QgsCoordinateTransform, 
    QgsLayoutExporter, QgsCoordinateReferenceSystem, QgsApplication, QgsGeometry,
    )
from time import sleep

# TELES files to check if complete
FILES = ['Epicenter.DBF', 'Epicenter.ID', 'Epicenter.MAP', 'Epicenter.TAB',
        'Evt1GbsTownLoss.TAB', 'Evt1GbsTownLoss.DBF',
        'ScenarioData.TAB', 'ScenarioData.DBF', 'Evt1GmDmnd.TAB', 'Evt1GmDmnd.DBF']
FILES_NO_CAS = ['Epicenter.DBF', 'Epicenter.ID', 'Epicenter.MAP', 
                'Epicenter.TAB', 'ScenarioData.TAB', 'ScenarioData.DBF']
FILES_NO_CAS = ['Epicenter.TAB', 'ScenarioData.TAB']
FILES = ['Epicenter.TAB', 'Evt1GbsTownLoss.TAB', 'ScenarioData.TAB', 'Evt1GmDmnd.TAB']
KEEP_FILES = ['users', 'users.csv', 'users_groups.csv', 'project.qgz', 'project_no_cas.qgz', '.gitignore']

NO_CAS = False

def main(target_dir, output_dir, dpi, WORKING_DIR):
    """
    main function
    """
    if not check_files(target_dir):
        return
    update_working_dir(target_dir, WORKING_DIR)
    # start qgis with no gui
    QgsApplication.setPrefixPath(r'C:/OSGeo4W/apps/qgis-ltr', True)
    qgs =  QgsApplication([], False)
    qgs.initQgis()
    project = QgsProject.instance()
    year = os.path.basename(os.path.dirname(target_dir))[1:5]
    PROJECT_PATH = os.path.join(WORKING_DIR, 'project.qgz')
    PROJECT_NO_CAS_PATH = os.path.join(WORKING_DIR, 'project_no_cas.qgz')
    # load project
    if NO_CAS:
        project.read(PROJECT_NO_CAS_PATH)
    else:
        project.read(PROJECT_PATH)
    layout_setting(project, year)
    filename_ = os.path.split(target_dir)[-2] + '_' + os.path.split(target_dir)[-1] + '.png'
    filename = os.path.join(output_dir, os.path.split(filename_)[-1])
    export_image(project, filename, dpi)
    qgs.exitQgis()
    clear_working_dir(WORKING_DIR)

def check_files(dir):
    """
    check TELES files complete and exists, and if there's no casualties
    """
    # check complete
    global NO_CAS
    if not os.path.exists(os.path.join(dir, 'Complete.txt')):
        print(f"Not complete yet! (No {os.path.join(dir, 'Complete.txt')})")
        return False
    with open(os.path.join(dir, 'Complete.txt'), 'r', encoding='utf-8') as f:
        state = str(f.read()).strip()
    if state != '1':
        print(f"Not complete yet! state:{state}")
        return False
    # check if all files exists
    # self.project_path = PROJECT_PATH
    NO_CAS = False
    for file in FILES:
        if not os.path.exists(os.path.join(dir, file)):
            print(f"{file} not found in {dir}, using no casualties project")
            NO_CAS = True
            break
    for file in FILES_NO_CAS:
        if not os.path.exists(os.path.join(dir, file)):
            print(f"{file} not found in {dir}, please check again")
            return False
    return True

def export_image(project, filename, dpi):
    """
    use QgsLayoutExporter to export image
    """
    layout = project.layoutManager().layoutByName('rosa_line_notify')
    output_path = filename
    setting = QgsLayoutExporter.ImageExportSettings()
    setting.dpi = dpi
    exporter = QgsLayoutExporter(layout)
    result = exporter.exportToImage(output_path, setting)
    if result == QgsLayoutExporter.Success:
        print(output_path, 'success')

    setting = QgsLayoutExporter.ImageExportSettings()
    setting.dpi = 48
    _path = output_path[:-4] + "_s.jpg"
    result = exporter.exportToImage(_path, setting)
    if result == QgsLayoutExporter.Success:
        print(_path, 'success')

def layout_setting(project, year):
    """
    set layout label text and map extent
    """
    layout = project.layoutManager().layoutByName('rosa_line_notify')
    layout.initializeDefaults()
    # set map extent
    _map = layout.itemById('Map 1')
    # has casuality
    if NO_CAS != True:
        town_layer = project.mapLayersByName('推估損失 (鄉鎮市區)')[0]
        layer = project.mapLayersByName('Evt1GbsTownLoss')[0]
        # filter town layer
        towns_to_show = []
        for feature in layer.getFeatures():
            towns_to_show.append(feature['town_code'])
        # int to str
        towns_to_show = [str(i) for i in towns_to_show]
        town_layer.setSubsetString(f"Town_code IN {tuple(towns_to_show)}")
        _extent = town_layer.extent()
        src = QgsCoordinateReferenceSystem(3826)
        dst = QgsCoordinateReferenceSystem(4326)
        tr = QgsCoordinateTransform(src, dst, project)
        extent = QgsGeometry.fromRect(_extent)
        extent.transform(tr)
        _map.zoomToExtent(extent.boundingBox())

    # set label text
    layer = project.mapLayersByName('ScenarioData')[0]
    for feature in layer.getFeatures():
        if feature['Name'] == 'SmsDateTime2':
            _item = layout.itemById('date_time')
            _text = QDateTime.fromString(year + str(feature['Value']), 'yyyyMM/dd_hh:mm').toString('yyyy/MM/dd hh:mm')
            _item.setText(_text)
        elif feature['Name'] == 'SmsEpicLoc':
            _item = layout.itemById('epicenter')
            _text = feature['Value']
            _item.setText(_text)
        elif feature['Name'] == 'SmsRespLevel':
            _item = layout.itemById('resp_level')
            _text = feature['Value']
            _item.setText(_text)
        elif feature['Name'] == 'SmsEpicX':
            EpicX = feature['Value']
        elif feature['Name'] == 'SmsEpicY':
            EpicY = feature['Value']
        elif feature['Name'] == 'SmsDepth':
            _item = layout.itemById('depth')
            _item.setText(f'{feature["Value"]} km')
        elif feature['Name'] == 'SmsMag':
            _item = layout.itemById('magnitude')
            _item.setText(f'{feature["Value"]}')
    _item = layout.itemById('coordinate')
    _item.setText(f'{EpicX}°E, {EpicY}°N')

def update_working_dir(target_dir, WORKING_DIR):
    """
    move files to working dir
    """
    clear_working_dir(WORKING_DIR)

    # copy all files in target dir to working dir
    for file in os.listdir(target_dir):
        _path = os.path.join(target_dir, file)
        # move file to project folder
        _new_path = os.path.join(WORKING_DIR, file)
        shutil.copy(_path, _new_path)

    # if NO_CAS != True:
    #     for file in FILES:
    #         _path = os.path.join(target_dir, file)
    #         # move file to project folder
    #         _new_path = os.path.join(WORKING_DIR, file)
    #         shutil.copy(_path, _new_path)
    # else:
    #     for file in FILES_NO_CAS:
    #         _path = os.path.join(target_dir, file)
    #         # move file to project folder
    #         _new_path = os.path.join(WORKING_DIR, file)
    #         shutil.copy(_path, _new_path)

def clear_working_dir(WORKING_DIR):
    """
    remove all files in working dir
    """
    for file in os.listdir(WORKING_DIR):
        if file not in KEEP_FILES:
            os.remove(os.path.join(WORKING_DIR, file))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', type=str, required=True)
    parser.add_argument('--output', '-o', type=str, default='default')
    parser.add_argument('--dpi', '-d', type=int, default=300)
    parser.add_argument('--working_dir', '-w', type=str, default='default')
    target_dir = parser.parse_args().input
    output_dir = parser.parse_args().output
    if output_dir == 'default':
        output_dir = os.path.join(os.path.dirname(__file__), os.path.pardir, "database", "images")
    working_dir = parser.parse_args().working_dir
    if working_dir == 'default':
        working_dir = os.path.join(os.path.dirname(__file__), 'line_notify_working_dir')
        # check if project file .qgz exists
    if not os.path.exists(os.path.join(working_dir, 'project.qgz')):
        print("project.qgz not found, please check argument --working_dir")
        exit()
    dpi = parser.parse_args().dpi

    main(target_dir, output_dir, dpi, working_dir)