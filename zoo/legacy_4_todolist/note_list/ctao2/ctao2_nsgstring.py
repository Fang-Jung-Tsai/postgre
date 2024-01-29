class nsgstring:

    def __init__(self):
        self.counter = 0

    def to_nsgkey(self, interger_index = None):
        self.counter = self.counter + 1

        if interger_index is None:
            #print ('nsg_{:08X}' .format  ( integer_index) )
            return 'nsg_{:08X}' .format  ( integer_index) 
        else:
            #print ('nsg_{:08X}' .format  ( self.counter) )
            return 'nsg_{:08X}' .format  ( self.counter)


def alnum(your_string):
    import re
    your_string = re.sub(r':'       ,'_nsg_disk_', your_string)
    your_string = re.sub(r'\\.'       ,'_nsg_dot_', your_string)
    your_string = re.sub(r'/'       ,'_nsg_path_', your_string)
    your_string = re.sub(r'\\'      ,'_nsg_path_', your_string)
    your_string = re.sub(r'\W+'     ,'_nsg_none_', your_string)
    return your_string

def alnum_uuid():
    """Returns a random string of length string_length."""
    import uuid

# Convert UUID format to a Python string.
    random = str(uuid.uuid4())

# Make all characters uppercase.
    random = random.upper()

# Remove the UUID '-'.
    random = random.replace("-","_")

# Return the random string.
    return random

def check_module():
    print (alnum('g:\\NCREE_GIS\\streetblock.sqlite') )
    print (alnum_uuid() )
    
if __name__ == '__console__' or __name__ == '__main__':
    check_module()
  