#coding=utf-8
#import .core moudel from the current file's folder if it has parent package
from .core import user_argument

class DynamoDB ():

    @staticmethod
    def decimal_dataframe(dataframe):
        #Because aws.dynamoDB only accept decimal type,
        #it needs to convert all data to decimal if its type is float 
        #dataframe.applymap(): apply function to all columns
        import decimal
        for column in dataframe.columns:
            if dataframe[column].dtype == 'float64':
                dataframe[column] = dataframe[column].apply(lambda x: decimal.Decimal(str(x)))
    
        return dataframe
    
    def __init__(self, table_name:str, region_name:str):
        import boto3
        user = user_argument()

        session = boto3.Session(
            aws_access_key_id       = user.ROSA_AWS_ACCESS_KEY_ID,
            aws_secret_access_key   = user.ROSA_AWS_SECRET_ACCESS_KEY,
            region_name=region_name
        )

        dynamodb = session.resource('dynamodb')
        self.table_name = table_name
        self.table      = dynamodb.Table(table_name)
    
    def scan(self, **kwargs):
        response = self.table.scan(**kwargs)
        return response
    
    def query(self, **kwargs):
        response = self.table.query(**kwargs)
        return response
    
    def put_item(self, **kwargs):
        response = self.table.put_item(**kwargs)
        return response
    
    def delete_item(self, **kwargs):
        response = self.table.delete_item(**kwargs)
        return response

    def overwrite (self, dataframe):
        pk_list     = []                    
        dataframe   = DynamoDB.decimal_dataframe(dataframe)
        primary_key = dataframe.index.name

        projection_expression = primary_key
        response = self.scan(ProjectionExpression=projection_expression)
        if response is not None:
            pk_list = [item[primary_key] for item in response['Items']]

        for key, value in dataframe.iterrows():
            if key not in pk_list:
                print(f'{primary_key}:{key} is added to dynamoDB {self.table_name}')
            else:
                print(f'{primary_key}:{key} is already in dynamoDB {self.table_name}')
            
            argu=value.to_dict()
            argu[primary_key] = key
            self.put_item(Item=argu)

    def save_new_item(self, dataframe):
        dataframe = DynamoDB.decimal_dataframe(dataframe)
        primary_key = dataframe.index.name

        pk_list = []            
        projection_expression = primary_key
        response = self.scan(ProjectionExpression=projection_expression)

        if response is not None:
            pk_list = [item[primary_key] for item in response['Items']]

        #pk_list->old_set , dataframe.index -> new_set
        #compare old_set and new_set to find new item
        #pick up new item from dataframe
        old_set     = set(pk_list)
        new_set     = set(dataframe.index)
        new_item    = new_set - old_set
        #convert set to list

        dataframe   = dataframe.loc[list(new_item)]
        
        if dataframe.empty:
            print(f'No new item is added to dynamoDB {self.table_name}')
            
        for key, value in dataframe.iterrows():
            print(f'{primary_key}:{key} is added to dynamoDB {self.table_name}')           
            argu=value.to_dict()
            argu[primary_key] = key
            self.put_item(Item=argu)

