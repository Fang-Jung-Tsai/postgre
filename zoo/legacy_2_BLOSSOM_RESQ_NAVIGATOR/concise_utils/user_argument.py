import os, socket, platform,  multiprocessing

###########################################################################
# this class is used to get the user argument from json file
# if the json file is not exist, then use the default value
# the json file is located at ~/XXXX_ROOT/user_argument.json
# XXXX is defined by the last folder name of the current file path
# the json file format like as below:
# {
#     "factory": "D:\\Downloads",
#     "ID": "123456789",
#     "key": "123456789"
#   }

class user_argument:

    def __init__(self):
        import json
        import os, socket, platform,  multiprocessing

        self.callname   = __name__
        self.filename   = __file__
        self.workdir    = os.getcwd()
        self.username   = os.getlogin()
        self.platform   = platform.platform()
        self.hostname   = socket.gethostname()
        self.cpu_size   = multiprocessing.cpu_count() 
        self.home       = os.path.expanduser("~")
        self.root       = os.path.join(self.home,'Desktop', 'ROSA_WAREHOUSE') 
        self.factory    = os.path.join(self.home,'Desktop', 'ROSA_FACTORY')

        file_path = os.path.join(self.root, 'user_argument.json')
        
        if os.path.exists(self.root) and os.path.exists(file_path) and os.path.isfile(file_path):
            self.has_user_argument_json  = True
            
            with open(file_path, 'r', encoding='utf8') as file:
                data = json.load(file)

                for key, value in data.items():
                    setattr(self, key, value)
                
                if 'FONT_CHINESE_PATH' in self.__dict__ :
                    self.font_chinese_path = os.path.join(self.root, self.FONT_CHINESE_PATH)
        else:
            self.has_user_argument_json  = False
            
#alias of user_argument
baseobject      = user_argument
hostmetadata    = user_argument
configuration   = user_argument


