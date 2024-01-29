import time
import os
import shutil
from  core import  configuration
from  core import  string
from qgis.core import( 
    QgsProject, 
    QgsCoordinateReferenceSystem, 
    QgsCoordinateTransform, 
    QgsLayoutExporter, 
    QgsApplication, 
    QgsGeometry)

class project:
    def __init__(self, project_folder):
        self.conf       = configuration()
        self.basename   = os.path.basename(project_folder)

        self.uuid       = 'ada_qgis_project' + string.alnum_uuid()
        self.working_d = os.path.join(self.conf.factory, self.uuid)

        shutil.copytree(project_folder, self.working_d) 
      
        QgsApplication.setPrefixPath( self.conf.qgis_PrefixPath, True)
        self.qgs =  QgsApplication([], False)
        self.qgs.initQgis()
        self.project = QgsProject.instance() 
    
    def layout_to_png(self, LAYOUT_NAME):

        project_path = os.path.join(self.working_d, 'project.qgz')
        output_path  = os.path.join (self.working_d, self.uuid + '.png' )

        self.project.read( project_path )
        
        self.layouts = QgsProject.instance().layoutManager().layouts()

        layout = QgsProject.instance().layoutManager().layoutByName(LAYOUT_NAME)
        layout.initializeDefaults()

        setting = QgsLayoutExporter.ImageExportSettings()
        setting.dpi = 300
        exporter = QgsLayoutExporter(layout)
        result = exporter.exportToImage(output_path, setting)
        time.sleep(1)

        if result == QgsLayoutExporter.Success:
            print('export success')
        else:
            print(QgsLayoutExporter.Success)
 
        self.qgs.exitQgis()
        time.sleep(1)

        return output_path


if __name__ == '__console__' or __name__ == '__main__':

    conf = configuration()
    SOURCE_DIR = os.path.join(conf.lily_root, 'db', 'qgis_project_cwb6069') 

    p = project(SOURCE_DIR)
    p.layout_to_png('for_linebot')