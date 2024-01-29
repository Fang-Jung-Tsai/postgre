import pkg_resources
import json


import json
import subprocess

# class ModuleManager:
#     def __init__(self):
#         self.installed_modules = []
    
#     def load_installed_modules(self):
#         try:
#             with open('installed_module.json', 'r') as f:
#                 self.installed_modules = json.load(f)
#         except FileNotFoundError:
#             self.installed_modules = []

#     def save_installed_modules(self):
#         with open('installed_module.json', 'w') as f:
#             json.dump(self.installed_modules, f)

#     def check_module_installed(self, module_name):
#         return module_name in self.installed_modules

#     def install_module(self, module_name):
#         if not self.check_module_installed(module_name):
#             subprocess.check_call([sys.executable, "-m", "pip", "install", module_name])
#             self.installed_modules.append(module_name)
#             self.save_installed_modules()

#     def install_all_modules(self):
#         with open('requirements.txt', 'r') as f:
#             required_modules = f.read().splitlines()
        
#         for module in required_modules:
#             self.install_module(module)


class module:
    def __init__(self, module_list_file='installed_module.json'):
        self.module_list_file = module_list_file

    def scan_installed_modules(self):
        installed_modules = [d.key for d in pkg_resources.working_set]
        with open(self.module_list_file, 'w') as f:
            json.dump(installed_modules, f)

    def uninstall_all_modules(self):
        with open(self.module_list_file, 'r') as f:
            installed_modules = json.load(f)
        for module in installed_modules:
            pkg_resources.require(module)[0].uninstall()

    def upgrade_all_modules(self):
        with open(self.module_list_file, 'r') as f:
            installed_modules = json.load(f)
        for module in installed_modules:
            pkg_resources.require(module)[0].update()

    def check_module_installed(self, module_name):
        return module_name in self.installed_modules

    def install_all_modules(self):
        with open(self.module_list_file, 'r') as f:
            installed_modules = json.load(f)



        for module in installed_modules:
            pkg_resources.require(module)[0].install()


# # 使用範例
if __name__ in ['__main__']:
    import os    
    import sys

    #TOFIX module 這裡有循環引用的問題 
    sys.path.append('..')
    from Alpha.core import user_argument
    #TOFIX module 這裡有循環引用的問題

    config = user_argument()
    module_manager = module(os.path.join(config.lily_root, 'installed_module.json') )
                            
    module_manager.scan_installed_modules()
    #module_manager.upgrade_all_modules()


# # 掃描並存儲已安裝的模組列表
# module_manager.scan_installed_modules()

# # 解除安裝所有模組
# module_manager.uninstall_all_modules()

# # 升級所有模組
# module_manager.upgrade_all_modules()

# # 安裝所有模組
# module_manager.install_all_modules()
