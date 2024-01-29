#coding=utf-8

###########################################################################
# this class is used to get the user argument from json file
# if the json file is not exist, then use the default value
# the json file is located at ~/LILY_ROOT/user_argument.json
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
        self.hostname   = socket.gethostname()
        self.platform   = platform.platform()
        self.cpu_code   = multiprocessing.cpu_count() 
        self.workdir    = os.getcwd()
        self.home       = os.path.expanduser("~")
        self.username   = os.getlogin()

        self.lily_root          = os.path.join(self.home, 'LILY_ROOT')  # LILY_ROOT        
        self.has_pylily_json    = False
        file_path = os.path.join( self.lily_root, 'user_argument.json')

        if os.path.exists(file_path) and os.path.isfile(file_path):

            self.has_pylily_json = True

            with open(file_path, 'r', encoding='utf8') as file:
                data = json.load(file)

                for key, value in data.items():
                    setattr(self, key, value)

                # check/create if not exists factory directory
                if 'factory' in self.__dict__ and not os.path.exists(self.factory):
                    os.mkdir(self.factory)
                else:
                    self.factory = os.path.join(self.home, 'Downloads')

###########################################################################
# this class is used to define a string as unikey  for each object
# the string format is nsg_XXXXXXXX
# the string is used to identify the object
# the string is used to create a unique (file name, directory name, key,...)
class string:

    def __init__(self):
        self.counter = 0

    def to_nsgkey(self, integer_index = None):
        self.counter = self.counter + 1

        if integer_index is not None:
            return 'nsg_{:08X}' .format  (integer_index) 
        else:
            return 'nsg_{:08X}' .format  (self.counter)

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

###########################################################################
## The answer to the Ultimate Question of Life, the Universe, and Everything"
class answer:
    def __init__(self):
        import inspect  
        from datetime import datetime
        curframe        = inspect.currentframe()
        calframe        = inspect.getouterframes(curframe, 2)

        # get stack info from calframe
        caller_name     = str(calframe[1][0])
      
        self.uuid               = string.alnum_uuid()
        self.begtime            = datetime.now()
        self.msgtime            = [[ self.begtime, 0.0, caller_name ]]            

    def today(self)->str:
        return self.host.today.strftime("%Y%m%d")

    def now(self)->str:
        return self.host.today.strftime("%Y%m%d%H%M%S")
        
    def tick(self):
        import inspect
        from datetime import datetime
        curframe        = inspect.currentframe()
        calframe        = inspect.getouterframes(curframe, 2)

        # get stack info from calframe
        caller_name     = str(calframe[1][0])

        time_point      = datetime.now()
        time_diff       = (time_point - self.begtime)

        seconds         = (time_diff).seconds 
        ms              = (time_diff).microseconds 
        self.msgtime.append( [ time_point, seconds,  caller_name ] )

        return '{2}_({0:03}s).({1:06}ms)\t\t'.format(seconds, ms, self.uuid)
    
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


###########################################################################
# this class is used to get the data from webapi
# the data format is json
class webapi:
    def __init__(self, url:str, headers=None):  
        import re
        import requests
        self.url = url
        reqhead = {
        'Connection': 'Keep-Alive',
        'Accept': 'application/json, text/html, application/xhtml+xml, */*',
        'Accept-Language': 'en-US,en;q=0.8,zh-TW;q=0.5',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35'
        }

        if headers is not None:
            reqhead.update(headers)

        self.response = requests.get(url , headers = reqhead , verify=False )    
        self.type = self.response.headers.get('Content-Type')

        if re.match('^application/json.*', self.type):
            self.json = self.response.json()
        else:
            self.json = None


###########################################################################
# my Exception class
class TODEL (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TODEL(功能已經移除)'''
        return 

class TOFIX (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO FIX(有BUG)'''
        return 

class TODEV (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO DEV(發展中的功能)'''
        return
    
class TOADD (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO ADD(未來要加入的功能)'''
        return
#


###########################################################################
# list of all classes in this module
# (user_argument, string, webapi, answer, TODEL, TOFIX, TODEV, TOADD)

#alias of user_argument
baseobject      = user_argument
hostmetadata    = user_argument
configuration   = user_argument

#alias of string
nsgstring  = string
nsgkey     = string

#alias of webapi
urlstring = webapi
urlapi    = webapi

#alias of answer
timer           = answer
ctao3_42        = answer

if __name__ == '__main__':

    #check all attributes and methods of user_argument
    conf = user_argument()

    for key, value in conf.__dict__.items():
        print(key, '=', value)

    #chcek all attributes and methods of string
    str1 = string()
    print (str1.to_nsgkey() )
    print (str1.to_nsgkey(1) )
    print (str1.to_nsgkey(2) )
    print (string.alnum('G:\\NCREE_GIS\\streetblock.sqlite') )
    print (string.alnum_uuid() )

    #check all attributes and methods of webapi
    #if conf has cwb_key, then use it, else use the default value
    url = 'www.google.com'
   
    if hasattr(conf, 'cwb_key'):
        url = f'https://opendata.cwb.gov.tw/api/v1/rest/datastore/F-D0047-061?Authorization={conf.cwb_key}&format=JSON&sort=time'
        
    web = webapi(url)
    print(web.response.status_code)
    print(web.type) 

    if web.json is not None:
        print(web.json)

    #check all attributes and methods of answer
    ans = answer()
    print(ans.tick())   
    print(ans.tick())
    lst1 = [1,2,3,4,5,6,7,8,9,10,11,12,13]

    new_list = ans.numbering_list(lst1)

    print('nparts',ans.split_list_to_n_parts(lst1, 4))
    print('chunks',ans.split_list_to_chunks(lst1, 5))

    print(new_list)
    