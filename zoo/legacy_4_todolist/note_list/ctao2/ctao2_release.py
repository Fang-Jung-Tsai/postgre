#!/usr/bin/env python3.7
# Last modified: 20200509 by Cheng-Tao Yang
# ctao2
# coding=utf-8
import os
import shutil
from Lily.ctao2.ctao2_hostmetadata import hostmetadata

class release(hostmetadata):

    def __init__(self, pylily_path = None):        
        super().__init__()

        if pylily_path == None:
            self.pylily_path = self.factory +'/release_pylily'
        else:
            self.pylily_path = pylily_path

        if  not os.path.exists(self.pylily_path) :
            os.mkdir(self.pylily_path)

    def clone_module(self, module):
        from pathlib import Path

        file = module.__file__
        name = module.__name__
        path = name.split('.')
  
        module_path = self.pylily_path

        for subpath in path[:-1]:
            module_path = module_path + '/' + subpath
            initfile    = module_path +'/__init__.py'

            if  not os.path.exists(module_path) :
                os.mkdir(module_path)
            
            if  not os.path.isfile(initfile):
                Path(initfile).touch()

        shutil.copy(file, module_path)

    def release_pylily(self):
        import Lily.ctao2.ctao2_database
        import Lily.ctao2.ctao2_database_dialogue
        import Lily.ctao2.ctao2_database_mediator
        import Lily.ctao2.ctao2_database_sunday
        import Lily.ctao2.ctao2_digraph
        import Lily.ctao2.ctao2_nsgstring
        import Lily.ctao2.ctao2_urlstring
        import Lily.ctao2.ctao2_filefeature
        import Lily.ctao2.ctao2_release 

        self.clone_module(Lily.ctao2.ctao2_database)
        self.clone_module(Lily.ctao2.ctao2_database_dialogue)
        self.clone_module(Lily.ctao2.ctao2_database_mediator)
        self.clone_module(Lily.ctao2.ctao2_database_sunday)
        self.clone_module(Lily.ctao2.ctao2_digraph)
        self.clone_module(Lily.ctao2.ctao2_nsgstring)
        self.clone_module(Lily.ctao2.ctao2_urlstring)
        self.clone_module(Lily.ctao2.ctao2_filefeature)
        self.clone_module(Lily.ctao2.ctao2_release)
        ########################################################################################
        ########################################################################################
        import Lily.ctao2.weekday.rescure_route_pkg.console_2
        import Lily.ctao2.weekday.rescure_route_pkg.step1_pre_node
        import Lily.ctao2.weekday.rescure_route_pkg.step2_pre_edge
        import Lily.ctao2.weekday.rescure_route_pkg.step3_postcost
        import Lily.ctao2.weekday.rescure_route_pkg.step4_check_network
        import Lily.ctao2.weekday.rescure_route_pkg.step5_mrrp
        import Lily.ctao2.weekday.rescure_route_pkg.step5_rrplanning

        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.console_2)
        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.step1_pre_node)
        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.step2_pre_edge)
        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.step3_postcost)
        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.step4_check_network)
        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.step5_mrrp)
        self.clone_module(Lily.ctao2.weekday.rescure_route_pkg.step5_rrplanning)
        ########################################################################################
        ########################################################################################
        import Lily.ctao2.rescure_route.rr_step0_calculate_cost
        import Lily.ctao2.rescure_route.rr_step1_pre_node
        import Lily.ctao2.rescure_route.rr_step2_pre_edge
        import Lily.ctao2.rescure_route.rr_step5_mrrp

        self.clone_module(Lily.ctao2.rescure_route.rr_step0_calculate_cost)
        self.clone_module(Lily.ctao2.rescure_route.rr_step1_pre_node)
        self.clone_module(Lily.ctao2.rescure_route.rr_step2_pre_edge)
        self.clone_module(Lily.ctao2.rescure_route.rr_step5_mrrp)
        ########################################################################################
        ########################################################################################

        import Lily.ctao2.console.console_merge
        import Lily.ctao2.console.console_t4

        self.clone_module(Lily.ctao2.console.console_merge)
        self.clone_module(Lily.ctao2.console.console_t4)
        ########################################################################################
        ########################################################################################

 
if __name__ == '__console__' or __name__ == '__main__':
    import sys

    release_path = None

    if len(sys.argv) == 2:
        print ('argument 2')
        print ('argument 2')
        print ('argument 2', sys.argv[1])
        release_path = sys.argv[1]
             

    release = release(release_path)
    release.release_pylily()
