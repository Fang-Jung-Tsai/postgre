#coding=utf-8
import abc

class webapi():

    @staticmethod
    def url_to_filename(url):
        import datetime
        import urllib.parse
        # Parse the URL into components
        parsed_url = urllib.parse.urlparse(url)

        now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  
        # Combine the netloc (network location) and path components
        # Replace any characters that are not suitable for Unix filenames
        filename = parsed_url.netloc + parsed_url.path
        filename = now_str + filename.replace('/', '_').replace(':', '_').replace('?', '_') + '.webapi'

        return filename
    
    @abc.abstractmethod    
    def __init__(self, url:str):
        import os  
        import re
        import json
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning

        #disable warning for https
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        self.url = url
        
        req_headers = {
                # 這個欄位用來控制是否維持網路連線。'Keep-Alive' 表示連線在完成請求後應保持開啟，以便進行進一步的請求。
            'Connection': 'Keep-Alive',
                # 在 'Accept' 欄位中加入 'image/jpeg', 'image/png', 'application/gzip' 來接受 JPEG 和 PNG 圖片以及 tar.gz 檔案
            'Accept': 'application/json, text/html, application/xhtml+xml, image/jpeg, image/png, application/gzip, */*',
                # 這個欄位用來告訴伺服器，客戶端的首選語言。這裡列出了英語（美國）和中文（台灣），並指定了各自的品質因數（q），表示相對的優先級。             
            'Accept-Language': 'en-US,en;q=0.8,zh-TW;q=0.5',                      
                # 這個欄位用來告訴伺服器，客戶端的軟體環境。這裡的值模擬了一個在 Windows 10 上運行的瀏覽器。
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35'  
        }

        self.response = requests.get(url , headers = req_headers , verify= False )    
        self.type = self.response.headers.get('Content-Type')

        #define outfile in user's home Download folder
        filename = self.url_to_filename(url)
        filename = os.path.join(os.path.expanduser('~'), 'Downloads', filename)        

        if re.match('^application/json.*', self.type):
            self.json = self.response.json()
            #write json to gz file
            with open(filename, 'w') as f:
                json.dump(self.json, f)

        elif re.match('^binary/octet-stream.*', self.type):
            try:
                self.binary = self.response.content
                #write binary to file
                with open(filename, 'wb') as f:
                    f.write(self.binary)

                #convert binary to string
                constring = self.binary.decode('utf-8')
                #remove linebreak in self.string
                constring = constring.replace('\r\n', '')
                #convert string to json
                self.json = json.loads(constring)
            except:
                self.json = None

        else:
            self.json = None