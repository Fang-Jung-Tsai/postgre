###########################################
# just   for ctao2_release
class ctao2_filefeature:
    __file__ = __file__

###########################################

class filefeature:
    def __init__(self, filename):
        import os, datetime

        self.f_dict = {'filename': filename}

        fullname, self.f_dict['extension'] = os.path.splitext(filename)
        self.f_dict['path'], self.f_dict['name'] = os.path.split( fullname )

        if os.path.isfile(filename):
            statinfo     = os.stat(filename)
            self.f_dict['size']  = statinfo.st_size
            self.f_dict['time_a']    = datetime.datetime.utcfromtimestamp(int( statinfo.st_atime))
            self.f_dict['time_m']    = datetime.datetime.utcfromtimestamp(int( statinfo.st_mtime))
            self.f_dict['time_c']    = datetime.datetime.utcfromtimestamp(int( statinfo.st_ctime))
    
    #filefeature.read_bytes()        
    def read_bytes(self):
        import hashlib

        try:
            #self.f_dict['bytes']    = open(filename, 'rb').read()
            bytes = open( self.f_dict['filename'], 'rb').read()
            self.f_dict['md5sum']   = hashlib.md5( bytes ).hexdigest()                
        except :
            #self.f_dict['bytes']    = None
            self.f_dict['md5sum']   = None

        return self.f_dict['md5sum']

    #filefeature.read_exif()
    def read_exif(self):
        import piexif, datetime
        self.f_dict['exif']  = True

        try:
            exif_dict    = piexif.load(self.f_dict['filename'])
     
            if  piexif.ImageIFD.Make in exif_dict['0th']:
                self.f_dict['exif_make']  = exif_dict['0th'][piexif.ImageIFD.Make].decode('utf-8') 
            #else:
            #    self.f_dict['exif_make']  = None

            if piexif.ImageIFD.Model in exif_dict['0th']:
                self.f_dict['exif_model'] = exif_dict['0th'][piexif.ImageIFD.Model].decode('utf-8')
            #else:
            #    self.f_dict['exif_model']  = None

            if  piexif.ExifIFD.DateTimeOriginal in exif_dict['Exif']:
                t1 = exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal]
                self.f_dict['exif_time_original'] = datetime.datetime.strptime(t1.decode('utf-8'), '%Y:%m:%d %H:%M:%S') 
            #else:
            #    self.f_dict['exif_time_original']  = None

            if  piexif.GPSIFD.GPSLongitude in exif_dict['GPS']:
                _x = exif_dict['GPS'][piexif.GPSIFD.GPSLongitude]
                self.f_dict['exif_longitude'] =     _x[0][0]/float(_x[0][1]) + (_x[1][0]/float(_x[1][1])/60.0) + (_x[2][0]/float(_x[2][1])/3600.0)
            #else:
            #    self.f_dict['exif_longitude'] = None

            if  piexif.GPSIFD.GPSLatitude in exif_dict['GPS']:
                _x = exif_dict['GPS'][piexif.GPSIFD.GPSLatitude]
                self.f_dict['exif_latitude'] =      _x[0][0]/float(_x[0][1]) + (_x[1][0]/float(_x[1][1])/60.0) + (_x[2][0]/float(_x[2][1])/3600.0)
            #else:
            #    self.f_dict['exif_latitude'] = None
                
        except :
            self.f_dict['exif']  = False

        return self.f_dict['exif']

    #filefeature.make_uuid()
    def make_uuid(self):
        #TODO
        import os, math, datetime

        size_s = hex(self.f_dict['size'])
        time_o = self.f_dict['exif_time_original']  if 'exif_time_original' in self.f_dict else self.f_dict['time_m']        
        time_y = time_o.strftime('%Y')
        time_s = time_o.strftime('%Y%m%d_%H%M%S')
        self.f_dict['target_dir1']     = time_y

        if 'exif_model' in self.f_dict:
            model  = self.f_dict['exif_model'] 
            make   = self.f_dict['exif_make']
            self.f_dict['target_dir2']     =  'CAMERA_'+ make.replace(' ', '0').replace( '\x00', '').zfill(8)[-8:] + '_' + model.replace(' ', '0').replace( '\x00', '').lower().zfill(32)[-32:]                               

            self.f_dict['target_filename'] = ( time_s +'_'+ size_s +'_'+ self.f_dict['md5sum'] + self.f_dict['extension'] ).upper()
        else: 
            drive, path     = os.path.splitdrive(self.f_dict['path'])
            path, subpath2  = os.path.split(path)
            path, subpath1  = os.path.split(path)
            self.f_dict['target_dir2']     =   'DOCUMENT_' + subpath1.replace(' ', '0').zfill(16)[-16:] + '_'  + subpath2.replace(' ', '0').zfill(16)[-16:]
            
            self.f_dict['target_filename'] = self.f_dict['name'] + '_' + time_s + '_' + size_s +'_'+ self.f_dict['md5sum'] + self.f_dict['extension']

            
        return [self.f_dict['target_dir1'], self.f_dict['target_dir2'], self.f_dict['target_filename']]



class directoryfeature:

    def __init__(self, target_directory):
        import os, pandas
        import re

        rdset_rows = []    
        for path, dirs, files in os.walk(target_directory):
            for fname in files:
                src_filename = os.path.join(path, fname)

                if re.match(r'''.*\$RECYCLE\.BIN.*''', src_filename):
                    print ('pass=', src_filename)
                    continue

                ff = filefeature(src_filename)
                ff.read_exif()

                #ff.read_bytes()
                #ff.make_uuid()
                
                rdset_rows.append(  ff.f_dict )
                print (src_filename)

        self.filelist = pandas.DataFrame.from_dict(rdset_rows, orient='columns') 
        return 


## TODO 執行這個目錄
## 執行指定目錄 指定database

## 可獨立執行
if __name__ == '__main__':
    import os , shutil
    dir = directoryfeature('''C:/Users/ctyang/Pictures''')
    print(dir)


