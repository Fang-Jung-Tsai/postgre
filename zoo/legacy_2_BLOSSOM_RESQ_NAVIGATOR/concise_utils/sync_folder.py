import os
import shutil

class sync_folder:

    def __init__(self, origin, destination):
        
        self.origin_path       = origin
        self.destination_path = destination

    def push(self):

        if not os.path.exists(self.destination_path):
            shutil.copytree(self.origin_path, self.destination_path)
            return

        #compare self.origin_path and self.destination_path, find out subfolders and files that are not in self.destination_path and copy them to self.destination_path
        for root, dirs, files in os.walk(self.origin_path):
            #check if subfolders are not in self.destination_path and make them if they are not
            for dir in dirs:
                source_dir = os.path.join(root, dir)            
                #soure_dir remove self.origin_path (prefix) and first character which is '\'
                subfolder  = source_dir.replace(self.origin_path, '')[1:]
                target_dir = os.path.join(self.destination_path, subfolder)

                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                    print ('making new folder {} '.format(target_dir)  )

            for file in files:
                source_file = os.path.join(root, file)
                #soure_file remove self.origin_path (prefix) and first character which is '\'
                subfile     = source_file.replace(self.origin_path, '')[1:]
                target_file = os.path.join(self.destination_path, subfile)
                
                if not os.path.exists(target_file):
                    shutil.copy(source_file, target_file)
                    print ('clone file {} to {}'.format(source_file, target_file)  )
                    
                elif os.path.exists(target_file):
                    if os.path.getmtime(source_file) > os.path.getmtime(target_file):
                        shutil.copy(source_file, target_file)
                        print ('update file: {} to {}'.format(source_file, target_file)  )
                else:
                    print ('file {} already exists'.format(target_file)  )
                    
    def delelte_outdated_file(self):

        #remove files and folders in self.destination_path that are not in self.origin_path
        for root, dirs, files in os.walk(self.destination_path):
            for file in files:
                d_file = os.path.join(root, file)
                #remove (prefix)_path and first character which is '\'
                subfile     = d_file.replace(self.destination_path, '')[1:]
                o_file = os.path.join(self.origin_path, subfile)
                
                if not os.path.exists(o_file):
                    os.remove(d_file)
                    print ('remove file {} '.format(d_file)  )

            #check if subfolders are in self.origin_path and remove them if they are not
            for dir in dirs:
                d_folder = os.path.join(root, dir)            
                #soure_dir remove self.origin_path (prefix) and first character which is '\'
                subfolder  = d_folder.replace(self.destination_path, '')[1:]
                o_folder = os.path.join(self.origin_path, subfolder)

                if not os.path.exists(o_folder):
                    shutil.rmtree(d_folder)
                    print ('remove folder {} '.format(d_folder)  )