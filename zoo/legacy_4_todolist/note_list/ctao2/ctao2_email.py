# coding=utf-8
import re
import time
import email
import gzip
import imaplib

from datetime import datetime, timedelta
from Lily.ctao2.ctao2_database     import database 
from Lily.ctao2.ctao2_hostmetadata import hostmetadata
from Lily.ctao2.ctao2_nsgstring    import nsgstring

##############################################################################
class email_message:
    def __init__(self, bytes_message):
        self.msg    = email.message_from_bytes(bytes_message)
        self.type   = self.msg.get_content_type()
        self.keys   = [ row[0] for row in self.msg.items() ]
        self.gzip_bytes  = gzip.compress(bytes_message)

        self.headers = {}
        for key in self.keys:
            mtext = self.mtext_decode( self.msg[key] ) 
            self.headers[key.lower()]  = [mtext[0], mtext[1], key]  
        
        #message-id
        self.id     = self.headers['message-id'][0]

        #data
        self.exdate = email.utils.parsedate_to_datetime( self.headers['date'][0] )

        #from
        #-------- 
        #if  'from' in self.headers:
        #    match = re.search(r'\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b', self.headers['from'][0], re.IGNORECASE)
        #    self.sender_email_address = None if match is None else match.group()
        #else:
        #    self.sender_email_address = None 
        #--------
        self.from_address       = email.utils.parseaddr(self.msg['From'] )        

        #attached files
        self.parts =[]

        for part in self.msg.walk():
            type2       = part.get_content_type()
            content     = None
            filename    = None
        
            if type2 in ['text/html' ,'text/plain']:
                mtext       = self.mtext_decode(part.get_payload())
                content     = gzip.compress( bytes(mtext[0], encoding ='utf-8') )

            if part.get_filename() is not None:
                filename    = self.mtext_decode(part.get_filename())[0]
                content     = gzip.compress(part.get_payload(decode=True))

            self.parts.append ( [type2, content, filename])

        return

    def mtext_decode(self, text):
        subject     = email.header.decode_header(text)
        text        = subject[0][0]
        code_type   = subject[0][1]
    
        if code_type in ('big5', 'utf-8', 'cp950', 'iso-8859-1') :
            text = text.decode(code_type)

        elif code_type in ('gbk', 'gb2312') :
            text = text.decode('gbk')

        elif code_type is None and isinstance(text, str):
            text = text

        elif code_type is None and isinstance(text, bytes):
            text = text.decode('utf-8') 

        elif code_type is not None and isinstance(text, bytes):
            text = 'unknown-charset' 

        return [text, code_type]


class check_email:
    def __init__(self, mailhost, account, password):
        self.mailhost = mailhost
        self.account  = account
        self.password = password
        self.imapServer = imaplib.IMAP4_SSL(self.mailhost, 993)

        self.rv, self.da = self.imapServer.login(self.account, self.password)
        self.imapServer.select()

    def __del__(self):
        self.imapServer.close()
        self.imapServer.logout()

    def read_host_email(self, message_pattern):

        host_msg = {}

        msgid_list = []
        for pattern in message_pattern:
            resp, items = self.imapServer.search( pattern[0], pattern[1], pattern[2] )    
            msgid_list.append (  set(items[0].split())  )
        
        msgid_set = set.intersection(*msgid_list)

        for id in msgid_set:
            result, data  = self.imapServer.fetch(id, "(RFC822)")
            msg           = email_message(data[0][1])
            host_msg[msg.id] = msg

        return host_msg

if __name__ ==  "__main__":
    m15   = (datetime.today() - timedelta(days=120)).strftime("%d-%b-%Y")
    maildata = check_email('imap.gmail.com', 'ctyang@hodala.tw', 'passwd')

    #host_msg = maildata.read_host_email ( [ [None, '''SENTSINCE {0}'''.format(m15), 'All'],
    #                                        [None, '''FROM {0}'''.format('einvoice@fia.gov.tw'), 'All'] ] ) 

    del maildata







    