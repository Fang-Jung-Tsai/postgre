import io, re, os
import datetime
import poplib, email
import pandas as pd
from email.header import decode_header

from ..concise_utils.aws_dynamodb  import dynamodb
from ..concise_utils.user_argument import user_argument

# List of possible encodings
encoding_list = ['cp950', 'big5', 'big5-hkscs','hz-gb-2312', 'utf-8', 'gb2312', 'gbk', 
                 'gb18030', 'euc-cn', 'euc-jp', 'shift_jis', 'GB2312', 
                 'iso-2022-jp','iso-2022-jp-2', 'iso-2022-kr', 'ISO-8859-1']

class mail_cwb_reports:

    def __init__(self):

        user = user_argument()
        self.mail = poplib.POP3_SSL('mail.narlabs.org.tw')
        self.mail.user(user.NCREE_EMAIL_USERNAME)
        self.mail.pass_(user.NCREE_EMAIL_PASSWORD)

        self.get_latest_emails()
        #eq_dict is email_dict remove st_list
        self.eq_df = pd.DataFrame()
        self.st_df = pd.DataFrame()

        for key, eq in self.email_dict.items():
            #concat st_list to self.st_df
            st = eq['st_list']
            st['subject']=key 
            st['subject_st'] = st['subject'] + '_' + st['Sta.']
            self.st_df = pd.concat([self.st_df, st], ignore_index=True)

            eq['st_list']   = eq['st_list'].shape[0] 
            eq['subject']   = key

            self.eq_df = self.eq_df.append(eq, ignore_index=True)

        if self.eq_df.empty or self.st_df.empty:
            print ('No new cwb report')
            return
    
        self.eq_df.set_index('subject', inplace=True)
        self.st_df.set_index('subject_st', inplace=True)

        #write data to file
        class_name = self.__class__.__name__ # current class name
        self.eq_df.to_csv(os.path.join(user.factory, f'{class_name}_eq_df.csv'))
        self.st_df.to_csv(os.path.join(user.factory, f'{class_name}_st_df.csv'))
        
        #upload to dynamoDB
        self.upload_to_dynamoDB()

    def get_latest_emails(self):
        # Get number of emails in mailbox
        num_emails      = len(self.mail.list()[1])

        # Get latest 100 emails
        start_email     = max(num_emails - 50, 1)
        end_email       = num_emails
        latest_emails   = range(start_email, end_email + 1)

        # Loop through latest N emails
        self.email_dict = {}

        for email_id in latest_emails:
            # fetch email data
            try:
                email_data  = self.mail.retr(email_id)
                iostream    = self.iostream(email_data[1])
                msg         = email.message_from_binary_file(iostream)

                # Get email subject and check if subject is start with 'CWB Earthquake Final Report,' 
                subject = msg['Subject']
                if not isinstance(subject, str) or not re.match(r'CWB Earthquake Final Report,', subject):
                    continue
                
                subject_re = re.compile(r'^CWB Earthquake Final Report,(.*?),(.*?)$')
                subject_match = re.match(subject_re, subject)
                if subject_match:
                    subject = subject_match.group(1) + '_' + subject_match.group(2)
    
                body = ''
                if msg.is_multipart():
                    for part in msg.get_payload():
                        if part.get_content_type() == 'text/plain':
                            body  = part.get_payload(decode=True).decode('utf-8') 

                if body != '' and subject is not None:
                    cwb_report = self.decode_CWB_Earthquake_Final_Report(body)
                    self.email_dict [subject] = cwb_report

            except Exception as e:
                continue

        return self.email_dict

    def upload_to_dynamoDB(self):
        eq = self.eq_df
        st = self.st_df

        dynamoDB_eq     = dynamodb('data_CWB_Earthqauke_Final_Report', 'us-east-1')
        dynamoDB_eq.save_new_item(eq)

        dynamoDB_sta    = dynamodb('data_CWB_Earthqauke_Final_Report_station', 'us-east-1')
        dynamoDB_sta.save_new_item(st)

    @staticmethod
    def iostream(data_bytes_list):
        stream = io.BytesIO()
        for binary_data in data_bytes_list:
            stream.write(binary_data)
            stream.write(b'\n')
        stream.seek(0)
        return stream

    @staticmethod
    def tofloat(str):
        try:
            return float(str)
        except ValueError:
            return None
        
    @staticmethod
    def decode_CWB_Earthquake_Final_Report(report_body):
        # put each line of report body into a dataframe as row
        lines = pd.DataFrame(columns=['text'])
        lines ['text'] = report_body.split('\n')
        
        dict = {}
        # search and read Magnitude. data sample is as follow:
        # Magnitude, ML=4.9
        mag_re = re.compile(r'^Magnitude,\s*ML=(\d+\.\d+)')
        mag_idx = lines['text'].str.match(mag_re).idxmax()
        mag    = re.match(mag_re, lines.iloc[mag_idx]['text'])
        if mag:
            dict['magnitude'] = float(mag.group(1))
            
        # search and read Origin Time. data sample is as follow:
        # Origin Time: 7/ 8/23 21:22: 2.2  (UT)
        otime_re = re.compile(r'^Origin Time:\s*(\d+)/\s*(\d+)/\s*(\d+)\s*(\d+)\s*:\s*(\d+):')
        otime_idx = lines['text'].str.match(otime_re).idxmax()
        otime   = re.match(otime_re, lines.iloc[otime_idx]['text'])
        if otime:
            month    = int(otime.group(1))
            day      = int(otime.group(2))
            year     = int(otime.group(3))
            hour     = int(otime.group(4))
            minute   = int(otime.group(5))
            dict['otime'] = datetime.datetime(year, month, day, hour, minute).strftime('%Y-%m-%d %H:%M')

        #search and read result time. data sample is as follow:
        #Get Result Time ===> 21:28:07
        rtime_re = re.compile(r'^Get Result Time\s*===>\s*(\d+:\d+:\d+)')
        rtime_idx = lines['text'].str.match(rtime_re).idxmax()
        rtime   = re.match(rtime_re, lines.iloc[rtime_idx]['text'])
        if rtime:
            dict['result_time'] = rtime.group(1)

        #Location: 21.78N 121.66E, Depth: 24.9  KM
        location_re = re.compile(r'^Location:\s*(\d+\.\d+)N\s*(\d+\.\d+)E,\s*Depth:\s*(\d+\.\d+)\s*KM')
        location_idx = lines['text'].str.match(location_re).idxmax()
        location    = re.match(location_re, lines.iloc[location_idx]['text'])
        if location:
            dict['lat'] = float(location.group(1))
            dict['lon'] = float(location.group(2))
            dict['depth'] = float(location.group(3))

        #data between Felt Region: and Remarks:
        st_beg_idx = lines['text'].str.match(r'^Felt Region:').idxmax() + 1
        st_end_idx = lines['text'].str.match(r'^Remarks:').idxmax() -1

        #check if there is no data between Felt Region: and Remarks:
        if st_beg_idx > st_end_idx:
            return dict
        
        felt_region = lines.iloc[st_beg_idx:st_end_idx]
        #let the first row as column name
        dict['st_list'] = pd.DataFrame(columns=felt_region.iloc[0]['text'].split()) 
        #the following rows as data
        for idx, row in felt_region.iloc[1:].iterrows():
            st_re = re.compile(r'^(\w+)\s*(\d+\.\d+)N\s*(\d+\.\d+)E\s*(\d+)\s*(\d*\.\d+)\s*(\d*\.\d+)\s*(\d*\.\d+)([.\-\d\s]*)$')
            st_match = re.match(st_re, row['text']) 
            if st_match:
                #create a list for st_match.groups
                stdict={}
                #key name list is (Sta.  Lat.   Lon.  Int.  PGA     PGV  Ep_Dis  P_Arr  S_Arr)
                stdict['Sta.']      = st_match.group(1)
                stdict['Lat.']      = st_match.group(2)
                stdict['Lon.']      = st_match.group(3)
                stdict['Int.']      = st_match.group(4)
                stdict['PGA']       = st_match.group(5)
                stdict['PGV']       = st_match.group(6)
                stdict['Ep_Dis']    = st_match.group(7)
                stdict['P_Arr']     = st_match.group(8)[:9]
                stdict['S_Arr']     = st_match.group(8)[9:]
     
                dict['st_list'] = dict['st_list'].append(stdict, ignore_index=True)
            else:
                print('staion data error / break')

        return dict


#print all email subjects
# email report_body example is as follows:
'''
Earthquake Final Report (Regional Network)
Central Weather Bureau (CWB), Taiwan, R.O.C.
This is Informal Information for Rapid
Dissemination, the Official Report will be 
Broadcasted by CWB, Taiwan, R.O.C., at http://www.cwb.gov.tw/ 
Magnitude, ML=4.9
Origin Time: 7/ 6/23 18: 3:56.2  (UT)
Get Result Time ===> 18:08:40
Location: 23.13N 121.18E, Depth: 11.0  KM
 
Felt Region:
Sta.  Lat.   Lon.  Int.  PGA     PGV  Ep_Dis  P_Arr      S_Arr
EHD  23.15N 121.21E 4    44.3     1.5    3.3   58.6
ECS  23.10N 121.22E 4    80.6     3.5    5.1   59.2
FULB 23.20N 121.29E 3    21.4      .8   13.8   60.2
ELD  23.19N 121.03E 4    26.9     1.1   17.5   60.1
CHK  23.10N 121.37E 3    17.3     1.0   19.7   61.1
EDH  22.97N 121.30E 3    10.9      .4   21.1   61.2
LONT 22.91N 121.13E 4    32.1     1.6   25.1   61.4
EYUL 23.35N 121.32E 2     3.0      .2   28.0   61.6
ECB  23.32N 121.45E 1     1.8      .1   34.5   63.3
TWG  22.82N 121.08E 2     5.5      .2   35.9   62.8       67.9
STYH 23.18N 120.78E 3    12.8      .3   41.6   64.0
TTN  22.75N 121.16E 2     3.3      .1   41.6   64.5       71.3
EHY  23.50N 121.33E 2     3.4      .1   44.3   63.7       69.4
ALS  23.51N 120.81E 1     2.2      .1   56.7   66.9       74.0
SLG  22.99N 120.65E 2     2.6      .0   57.0   66.7       73.8
LDU  22.67N 121.47E 2     4.2      .2   58.2   66.2       74.7
WTP  23.24N 120.62E 2     2.5      .1   58.9   67.4       75.4
SGS  23.08N 120.59E 1     2.3      .1   60.9   66.9       75.0
ECL  22.60N 120.96E 1     1.9      .0   63.1   66.2       74.6
EGFH 23.67N 121.43E 1     1.0      .0   64.9   66.4       74.4
CHN1 23.18N 120.53E 1     1.6      .1   67.4   69.1       78.4
WCKO 23.44N 120.60E 2     3.5      .2   68.5   69.1       78.1
SSD  22.74N 120.64E 1     1.5      .0   70.1   68.2
WHY  23.70N 120.85E 1      .9      .0   71.4   69.2       77.6
TWL  23.26N 120.50E 1     1.4      .1   71.6   69.8       79.7
SMG  22.71N 120.64E 1      .8      .0   72.4   68.5       76.8
CHN5 23.60N 120.68E 2     3.7      .2   73.4   69.8       79.0
SCS  22.89N 120.49E 1     2.0      .1   75.6   70.4
C015 23.35N 120.41E 2     6.5      .3   82.6   72.0       83.9
SGL  22.72N 120.50E 1     1.1      .1   83.3   71.8
CHN2 23.53N 120.47E 1     2.5      .2   85.2   72.5       84.3
SPT  22.68N 120.50E 1     1.2      .0   86.4   72.2
CHY  23.50N 120.43E 3     8.6      .6   87.0   72.5       85.0
WGK  23.68N 120.57E 2     2.5      .2   87.9   73.0       85.4
OWD  23.95N 121.18E 1      .9      .0   91.6   71.6
SSH  23.14N 120.29E 1     1.8      .1   91.7   74.1
WDL  23.71N 120.54E 1     1.4      .1   92.6   73.7       86.7
SSP  22.48N 120.57E 1      .9      .0   95.3   72.9
SNJ  22.75N 120.34E 1     2.1      .1   96.4   75.3       90.9
TAI1 23.04N 120.24E 1     2.4      .1   97.5   75.3       89.0
CHY1 23.46N 120.29E 1     1.9      .1   98.0   74.3       87.6
D009 22.87N 120.26E 1      .9      .0   98.5   75.8
WNT1 23.91N 120.68E 1      .9      .0  100.5   74.9       88.3
SCL  23.17N 120.20E 1     1.5      .0  100.7   75.3       89.9
CHN8 23.35N 120.22E 1     1.3      .0  101.3   74.8       89.1
TAI  22.99N 120.20E 1     2.1      .1  101.4
WTK  23.69N 120.39E 1     1.6      .1  102.2   74.9       88.8
WSL  23.52N 120.23E 1     1.9      .1  107.1   75.5
WSS  22.64N 120.26E 1      .8      .0  108.8   77.7
SCK  23.15N 120.09E 1     2.2      .2  112.4              92.6
WSF  23.64N 120.23E 1     1.0      .1  112.6   76.1
WHF  24.14N 121.27E 1     1.1      .0  112.9   75.0
WML  23.80N 120.22E 1     1.3     0.1  123.5
LAY  22.04N 121.56E 1      .8      .0  126.7   75.6

Remarks:
Sta.: Station code
Lat.: Latitude of station
Lon.: Longitude of station
Int.: Intensity (CWB Intensity Scale)
PGA: Peak ground acceleration in cm/sec^2
PGV: Peak ground velocity in cm/sec
Ep_Dis: Epicentral distance in km
P_Arr: P wave arrival time in sec
S_Arr: S wave arrival time in sec
'''