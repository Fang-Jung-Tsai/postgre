class urlstring:
    def __init__(self, url, nohead=True):  
        import io
        import requests

        self.url = url

        if nohead:
            self.response = requests.get(url)
        else:
            reqhead = {
            'Connection': 'Keep-Alive',
            'Accept': 'text/html, application/xhtml+xml, */*',
            'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'}

            reqhead = {
            'Connection': 'Keep-Alive',
            'Accept': 'text/html, application/xhtml+xml, */*',
            'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

            self.response = requests.get(url , headers = reqhead , verify=False )    

        self.byteio = io.BytesIO()
        
        for chunk in self.response.iter_content(chunk_size = 512 * 1024): 
            if chunk: # filter out keep-alive new chunks
                self.byteio.write(chunk)

    def to_file(self, filename):
        f = open (filename,'wb')
        f.write( self.byteio.getvalue())
        f.close()
        return 

    def to_str(self) :
        return str(self.to_ungzip()) 

    #def to_lzma_xz(self) :
    #    import lzma
    #    return lzma.compress(self.byteio.getvalue())

    def to_gzip(self) :
        import gzip
        return gzip.compress(self.byteio.getvalue())

    def to_ungzip(self):
        import gzip
        import binascii

        first2bytes = self.byteio.getvalue()[:2]
        if binascii.hexlify(first2bytes) == b'1f8b':
            return gzip.decompress(self.byteio.getvalue())
        else:
            return self.byteio.getvalue()

    def get_table(self, table_index):
        import re
        specified_table = self.get_html_tables()[ table_index ]
        return specified_table

    def get_html_tables(self):
        import pandas
        html_tables = pandas.read_html( self.to_str(), encoding='utf-8' )
        return html_tables

    def to_excel(self, excelfile):
        import pandas
        writer    = pandas.ExcelWriter( excelfile, engine = 'xlsxwriter')
        count = 0
        for df in self.get_html_tables():
            count = count + 1
            df.to_excel(writer, sheet_name = 'table_{0}'.format(count))
        writer.save()
        writer.close()
    
    def to_json(self):
        import json
        return json.loads(self.to_str())