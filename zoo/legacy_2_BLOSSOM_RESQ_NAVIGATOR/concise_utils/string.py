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



#alias of string
nsgstring  = string
nsgkey     = string


#alias of answer
timer           = answer
ctao3_42        = answer

