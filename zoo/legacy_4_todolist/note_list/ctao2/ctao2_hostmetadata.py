#!/usr/bin/env python3.7
# Last modified: 20200509 by Cheng-Tao Yang
# ctao2
# coding=utf-8

class hostmetadata:

    def __init__(self, arg_factory = None, arg_warehouse = None):
        import json
        import re, os, socket, platform, datetime, multiprocessing

        self.today      = datetime.datetime.today()
        self.callname   = __name__
        self.hostname   = socket.gethostname()
        self.platform   = platform.platform()[:10]


        # factory directory, warehouse directory, cpu_code number
        self.workdir    = os.getcwd()
        self.factory    = self.workdir + 'data.Lily.ctao2.factory'        
        self.warehouse  = self.workdir + 'data.Lily.ctao2.warehouse' 
        self.cpu_code   = multiprocessing.cpu_count()

        self.home       = os.path.expanduser("~")
        
        if os.path.exists(  self.home + '/pylily.json' ):
            with open (self.home + '/pylily.json', encoding='utf8') as jfile:
                user_arg = json.load(jfile)
                if 'factory' in user_arg:
                    self.factory        = user_arg['factory']        

                if 'warehouse' in user_arg:
                    self.warehouse      = user_arg['warehouse']  

                if 'cpu_code' in user_arg:
                    self.cpu_code       = user_arg['cpu_code']
     
        #check/create if not exists directory
        if  not os.path.exists(self.factory) :
            os.mkdir(self.factory)

        if  not os.path.exists(self.warehouse) :
            os.mkdir(self.warehouse)

    #def set_syspath(self):
    #    import sys
    #    self.file = __file__
    #    sys.path.append(r'C:/Users/ctyang/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/pylily_pi')

    def check_module(self):
        for key, value in self.__dict__.items():
            if key != 'hostlist':
                print (key)
                print (value)
                print ('-------------------------------')

if __name__ == '__console__' or __name__ == '__main__':
    import sys, os
    #hostmetadata

    thishost = hostmetadata()
    thishost.check_module()