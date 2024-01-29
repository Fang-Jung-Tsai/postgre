###########################################################################
# this class is used to get the data from webapi
# the data format is json
class web_api:
    def __init__(self, url:str, headers=None):  
        import re
        import json
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

        elif re.match('^binary/octet-stream.*', self.type):
            self.binary = self.response.content
            #convert binary to string
            constring = self.binary.decode('utf-8')
            #remove linebreak in self.string
            constring = constring.replace('\r\n', '')
            
            try:
                #convert string to json
                self.json = json.loads(constring)
            except:
                self.json = None

        else:
            self.json = None



