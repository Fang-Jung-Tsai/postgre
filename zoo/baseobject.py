###########################################################################
# this class is used to get the user argument from json file
# if the json file is not exist, then use the default value
# the json file is located at ~/.ssh/rosa_config.json
# XXXX is defined by the last folder name of the current file path
# the json file format like as below:
# { 
#    "ID":"rosa_ctyang",
#    "KEY":"--",
#    "FONT_CHINESE_PATH":"fonts/msjhbd.ttc",
#    "POSTGRES_HOST":"localhost",
#    "POSTGRES_PORT":"5432",
#    "POSTGRES_DATABASE":"rosa_readwrite",
#    "POSTGRES_USERNAME":"ctyang",
#    "POSTGRES_PASSWORD":"---------"
# }
# the .env file is located at the current folder
# the .env file format like as below:
# POSTGRES_HOST=localhost
# POSTGRES_PORT=5432
# POSTGRES_DATABASE=rosa_readwrite
# POSTGRES_USERNAME=ctyang
# POSTGRES_PASSWORD=---------

class baseobject:
    
    def __init__(self):
        import re, json
        import os, socket, platform,  multiprocessing 
        #get essential attributes from operating system
        self.callname   = __name__
        self.filename   = __file__
        self.username   = os.getlogin()
        self.platform   = platform.platform()
        self.hostname   = socket.gethostname()
        self.cpu_size   = multiprocessing.cpu_count()
        #directories for ROSA
        self.workdir    = os.getcwd()
        self.home       = os.path.expanduser("~")
        self.ssh        = os.path.join(self.home,'.ssh')
        self.download   = os.path.join(self.home,'Downloads')
        self.desktop    = os.path.join(self.home,'Desktop')
        self.root       = os.path.join(self.home,'Desktop', 'ROSA_WAREHOUSE') 
        self.factory    = os.path.join(self.home,'Desktop', 'ROSA_FACTORY')

        #check if all directories for are exist, if not, then create it
        for dir in [self.download, self.desktop, self.root, self.factory]:
            if not os.path.exists(dir):
                os.makedirs(dir)

        #check if rosa_config.json is exist 
        #check it it is regular file and readable  
        rosa_config_file = os.path.join(self.ssh, 'rosa_config.json')
        if os.path.exists(rosa_config_file) and os.path.isfile(rosa_config_file) and os.access(rosa_config_file, os.R_OK):
            with open(rosa_config_file, 'r', encoding='utf8') as file:
                data = json.load(file)
                for key, value in data.items():
                    #check if the key is valid
                    #if the key is valid and not exist in self, then add it to self
                    #if the key is not valid, then ignore it
                    #valid key is start with uppercase letter and only contain uppercase letter, number and underscore
                    if re.match('^[A-Z]{1}[A-Z0-9_]*', key) and not hasattr(self, key):
                        setattr(self, key, value)
        else:
            #if rosa_config.json is not exist, rise error
            raise FileNotFoundError(f'{rosa_config_file} is not exist or not readable')
        
        #check if .env file is exist in current directory
        #check it it is regular file and readable
        env_file = os.path.join(self.workdir, '.env')
        if os.path.exists(env_file) and os.path.isfile(env_file) and os.access(env_file, os.R_OK):
            from dotenv import dotenv_values
            self.dotenv_values = dotenv_values(env_file)
            for key, value in self.dotenv_values.items():
                #check if the key is valid
                #if the key is valid and not exist in self, then add it to self
                if re.match('^[A-Z]{1}[A-Z0-9_]*', key) and not hasattr(self, key):
                    setattr(self, key, value)
                    
        #check if all essential attributes are exist
        essential_attributes = ['FONT_CHINESE_PATH', 'ID', 'KEY']
        for att in essential_attributes:
            if not hasattr(self, att):
                raise AttributeError(f'{att} is not exist in rosa_config.json')
    
    @staticmethod
    def alnum(your_string):
        import re
        #replace all non-alphanumeric characters of your_string
        #with underscore
        your_string = re.sub(r' '       ,'_s_', your_string)
        your_string = re.sub(r'\.'      ,'_d_', your_string)
        your_string = re.sub(r':'       ,'_d_', your_string)
        your_string = re.sub(r'/'       ,'_p_', your_string)
        your_string = re.sub(r'\\'      ,'_p_', your_string)
        your_string = re.sub(r'\W+'     ,'_o_', your_string)
        return your_string

    @staticmethod
    def alnum_uuid():
        """Returns a random string of length string_length."""
        import uuid

        # Convert UUID format to a Python string.
        random = str(uuid.uuid4())

        # Make all characters uppercase.
        random = random.upper()

        # Remove the UUID '#'.
        random = random.replace("-","_")

        # Return the random string.
        return random

    @staticmethod
    def split_list_to_chunks(lst: list, n: int) -> list:
        #To divide a list into a list of smaller lists, each containing n(amount) elements, 
        #with any remaining elements placed in the final list
        return [ lst[i:i + n] for i in range(0, len(lst), n) ]
 
    @staticmethod
    def split_list_to_n_parts(lst: list, n: int) -> list:
        #Yes, you can divide a DataFrame into n parts with approximately equal numbers of elements. 
        #One way to achieve this is by using the np.array_split() function from the NumPy library.
        #https://www.kite.com/python/answers/how-to-split-a-list-into-n-equal-parts-in-python

        import numpy
        nparts = numpy.array_split(lst, n)

        # to convert the numpy array to list
        return [ list(x) for x in nparts ]
    
    @staticmethod
    def numbering_list(lst: list) -> list:
        #enumerate(lst, start=0) return object not list, so need to convert to list 
        return [ [num, row] for num, row in enumerate(lst, start=0) ]