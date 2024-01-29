# coding=utf-8

import boto.ses
import datetime
import datetime
import re, requests
import os.path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import  MIMEApplication

def crawler1():
    response = requests.get("https://new.cpc.com.tw/", verify=False)

    t_date  = datetime.datetime.now().strftime("%Y-%m-%d")

    op1 = re.findall(r'''<strong>([0-9\.]*)</strong>''', re.search(r'''<dl id="OilPrice1">(.*)</dl>''', response.text).group(1))
    op2 = re.findall(r'''<strong>([0-9\.]*)</strong>''', re.search(r'''<dl id="OilPrice2">(.*)</dl>''', response.text).group(1))
    

    column_name=[u'日期\t\t',u'指標原油\t',u'匯率\t',u'92\t',u'95\t',u'98\t',u'汽油\t',u'柴油\t',u'LPG\n']

    df = "".join(column_name) + t_date + '\t' + ("\t".join (op1)) + '\t' + ("\t". join (op2))

    return df

def send_email(subject="NS-FYI", email_body="", recipient=["jitin.yang@gmail.com","ctyang@hodala.tw"]):
    
    conn = boto.ses.connect_to_region('us-east-1',                                         
        aws_access_key_id='AKIAJCDY72W22H2SIORQ',
        aws_secret_access_key='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')

    t_date  = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    subject = subject + ":" + t_date

    email_body = email_body + crawler1() 

    a1 = conn.send_email('jitin.yang@trainworld.tw', subject, email_body, recipient) 


def send_amazon_email_with_attachment(subject="NS-FYI"):
   
    conn = boto.ses.connect_to_region('us-east-1',                                         
        aws_access_key_id='AKIAJCDY72W22H2SIORQ',
        aws_secret_access_key='HHh1X7dAqpf2FzTWBefWKOcdxLiI7v3jYY/jvcEG')

    dummy       = 'jitin.yang@trainworld.tw'
    recipients  = ['jitin.yang@gmail.com','ctyang@hodala.tw']
 
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = 'jitin.yang TrainWorld <jitin.yang@trainworld.tw>' 
    msg['To'] = ', '.join(recipients)


    msg.preamble = 'Multipart message.\n'

#    part1 = MIMEText(u"Attached is the report", 'plain')
    part2 = MIMEText('<html><body><p>hello world</p></body></html>', 'html')

#    msg.attach(part1)
    msg.attach(part2)

    f = open('ctao1.jpg', mode='rb')
    temp = f.read()
 
    part = MIMEApplication(temp) #read binary
    part.add_header('Content-Disposition', 'attachment', filename='ctao1.jpg')
    msg.attach(part)

    f2 = open('ctao2.jpg', mode='rb')
    temp2 = f2.read()
    part3 = MIMEApplication(temp2) #read binary
    part3.add_header('Content-Disposition', 'attachment', filename='ctao2.jpg')
    #part3.add_header('Content-Disposition', 'inline') 
    msg.attach(part3)
    
    f3 = open('check_email.txt','w')
    f3.write(msg.as_string())
    f3.close()

    result = conn.send_raw_email(msg.as_string(), source=msg['From'], destinations=recipients)

if __name__ == "__main__":
    send_email()