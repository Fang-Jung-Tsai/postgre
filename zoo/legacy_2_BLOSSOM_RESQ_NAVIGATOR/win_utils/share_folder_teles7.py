import pandas
# import geopandas as gpd
import os, re, sys, json
from datetime import datetime
from .share_folder import share_folder

class share_folder_teles7 (share_folder):
    #check if the project is complete
    def is_Complete(self, path):

        filename =  os.path.join(path,'Complete.txt')
    
        if os.path.exists(filename) and os.stat(filename).st_size !=0:
            with open(filename, 'r', encoding='utf-8') as f:
                line = int(f.read())
                if line == 1 : return True 
        else:
                return False

    #check files list pattern
    def check_files_list_pattern(self, files_list):
        
        if self.files_pattern1.issubset(files_list) and self.files_pattern2.issubset(files_list):
            return 2

        elif self.files_pattern1.issubset(files_list):
            return 1

        else:
            return 0
 
    def __init__(self, server, srcdir, user, password):

        super().__init__(server, user, password)
        self.connect()

        self.files_pattern1= set(["Complete.txt", "CompleteSms.txt", 
                              "Epicenter.DBF", "Epicenter.ID", "Epicenter.MAP", "Epicenter.TAB", 
                              "RtdRapidPga.DBF", "RtdRapidPga.TAB", 
                              "ScenarioData.DBF", "ScenarioData.TAB", 
                              "ScenDefn.DBF", "ScenDefn.TAB", 
                              "ShortMsgData.DBF", "ShortMsgData.TAB"])


        self.files_pattern2= set(["Evt1.DBF", "Evt1.ID", "Evt1.MAP", "Evt1.TAB", 
                              "Evt1Fault.DBF", "Evt1Fault.ID", "Evt1Fault.MAP", "Evt1Fault.TAB",
                              "Ar_Summary.jpg", "Ar_SummaryGBS.txt", "Ar_SummaryTreif.txt", 
                              "RtdErrorPatn.DBF", "RtdErrorPatn.TAB"])

        self.EarthquakeDF= pandas.DataFrame()
        
        for root, dirs, files in self.client.walk(srcdir):
            for dir in dirs:
                if re.match('D[0-9]{8}', root[-9:]) and re.match('E[0-9]{4}', dir[-5:]):    
                    #
                    
                    tgbs_project = os.path.join(root, dir)
                    files_list = os.listdir(os.path.join(root, dir))
                    files_list_pattern = self.check_files_list_pattern(files_list)  

                    dict_1 = {
                             'nsg_key':f'{root[-9:]}_{dir[-5:]}',
                             'pattern': files_list_pattern,
                             'datetime_tag': datetime.strptime(f'{root[-8:]}{dir[-4:]}','%Y%m%d%H%M') ,
                             'path': tgbs_project, 
                             'complete_tag': self.is_Complete(tgbs_project), 
                             'file_tag': json.dumps(files_list)
                             }
                    #
                    current_row = pandas.DataFrame(dict_1, index=[0])
                    self.EarthquakeDF = pandas.concat([self.EarthquakeDF, current_row], ignore_index=True)

        # #alter column file_tag to the last column
        cols = list(self.EarthquakeDF.columns.values)
        cols.pop(cols.index('file_tag'))
        self.EarthquakeDF = self.EarthquakeDF[cols+['file_tag']]        	
        #alter column (complete_tag and pattern) type to int
        self.EarthquakeDF['pattern'] = self.EarthquakeDF['pattern'].astype(int)
        self.EarthquakeDF['complete_tag'] = self.EarthquakeDF['complete_tag'].astype(int)




    



