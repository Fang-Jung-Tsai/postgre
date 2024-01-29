import os
import shutil
from utils.concise_utils.user_argument import user_argument as conf

## define target path
argu = conf()
##C:\Users\jitin\AppData\Roaming\QGIS\QGIS3\profiles\default\python\plugins\ROSA_release
try:
    plugin_folder = argu.ROSA_QGIS_PLUGIN
except:
    plugin_folder = os.path.expanduser('~/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/ROSA_release')
    
target_path = plugin_folder


## define source path
current_folder = os.path.dirname(os.path.realpath(__file__))
# get parent folder of current folder
parent_folder  = os.path.dirname(current_folder)
package_folder = os.path.join(parent_folder, 'ROSA_QGIS_PLUGIN_ZIP')    
source_path    = package_folder

if not os.path.exists(target_path):
    shutil.copytree(source_path, target_path)
else:
    #compare source_path and target_path, find out subfolders and files that are not in target_path and copy them to target_path
    for root, dirs, files in os.walk(source_path):
        #check if subfolders are not in target_path and make them if they are not
        for dir in dirs:
            source_dir = os.path.join(root, dir)            
            #soure_dir remove source_path (prefix) and first character which is '\'
            subfolder  = source_dir.replace(source_path, '')[1:]
            target_dir = os.path.join(target_path, subfolder)
            print ('subfolder:{} syncing'.format(subfolder)  )

            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
                print ('new: making directory {}'.format(target_dir)  )
            else:
                print ('subfolder:{} is already exists'.format(target_dir)  )

        for file in files:
            source_file = os.path.join(root, file)
            #soure_file remove source_path (prefix) and first character which is '\'
            subfile     = source_file.replace(source_path, '')[1:]
            target_file = os.path.join(target_path, subfile)
            print ('subfile:{} syncing'.format(subfile)  )
            
            if not os.path.exists(target_file):
                print ('new:copying {} to {}'.format(source_file, target_file)  )
                shutil.copy(source_file, target_file)
                
            elif os.path.exists(target_file):
                if os.path.getmtime(source_file) > os.path.getmtime(target_file):
                    print ('modified: copying {} to {}'.format(source_file, target_file)  )
                    shutil.copy(source_file, target_file)
                else:
                    #print ('{} is up to date'.format(target_file)  )
                    pass


