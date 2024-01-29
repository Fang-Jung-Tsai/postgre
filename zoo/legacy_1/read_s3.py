import os
import boto3
from lily.core import user_argument as user_argument

if __name__=='__main__':
    conf = user_argument()
    #connnet to s3 with access key and secret key in conf
    
    session = boto3.Session(
            aws_access_key_id       = conf.ROSA_AWS_ACCESS_KEY_ID,
            aws_secret_access_key   = conf.ROSA_AWS_SECRET_ACCESS_KEY,
            region_name='ap-northeast-1'
    )
    
    s3 = session.resource('s3')

    #connet to bucket 'linebot-followers' and list all files in it
    bucket = s3.Bucket('linebot-followers')

    user_list = {}
    for obj in bucket.objects.all():
        #print(obj.key)
        #download all files  to local
        s3.Bucket('linebot-followers').download_file(obj.key, os.path.join(conf.factory,obj.key))
        filepath = os.path.join(conf.factory,obj.key)

        fileobj     = open(os.path.join(filepath), "r")
        #get first and second line of the file and remove the \n
        firstline   = fileobj.readline().rstrip('\n')
        secondline  = fileobj.readline().rstrip('\n')
     
        #sepreate the file name and file extension
        filename, file_extension = os.path.splitext(obj.key)
        key             = filename
        dict_argu       = {'linebot_friend_uuid':key, 'datetime':secondline, 'linebot_friend_nickname':firstline}
        user_list[key]  = dict_argu  

    #dynamo = dynamo(table_name='data_rosa_linebot_friends', region_name='us-east-1')
    #dynamo.save_new_item(primary_key='linebot_friend_uuid', item_data=user_list)

    print(user_list)

