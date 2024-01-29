#copy specific files to the target directory

import os
import shutil

#get current file path
path = os.path.dirname(os.path.realpath(__file__))
#get last folder name ,and transform to upper case
folder_name = os.path.basename(path)

##############################################################################
files = ['webapi_cwb6068_eq.py']
##############################################################################

target_path = os.path.join(r'c:\users\jitin\Desktop', 'rosa.schedule')

if not os.path.exists(target_path):
    raise Exception(f'{target_path} does not exist')
else:
    for file in files:
        shutil.copy(file, target_path)
        print(f'copy {file} to {target_path} successfully')

##############################################################################
files_in_lily = ['lily/aws_adapter.py', 'lily/core.py']
##############################################################################

for file in files_in_lily:
    shutil.copy(file, os.path.join(target_path, 'lily'))
    print(f'copy {file} to {target_path} successfully') 