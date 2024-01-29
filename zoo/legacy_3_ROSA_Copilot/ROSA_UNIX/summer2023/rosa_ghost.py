import random
import boto3
import openai
import pandas as pd
import time
import os
import re
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
region_name = os.getenv("region_name")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
openai.api_key = os.getenv("openai.api_key")


def get_new_messages(region_name, aws_access_key_id, aws_secret_access_key):
    dynamodb = boto3.resource('dynamodb',
                              region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

    # Fetch data from data_command_to_rosa
    table1 = dynamodb.Table("data_command_to_rosa")
    response1 = table1.scan()
    command_timestamp = None
    if response1['Items']:
        command_df = pd.DataFrame(response1['Items'])
        #我想get maximun timestamp value
        command_timestamp = command_df['timestamp'].astype("int64").max()
    else:
        command_timestamp = 0
        print("No messages found in data_command_to_rosa.")

    # Fetch data from data_dmm_to_rosa
    table2 = dynamodb.Table("data_dmm_to_rosa")
    response2 = table2.scan()
    rosa_df = pd.DataFrame()
    if response2['Items'] and command_timestamp is not None:
        dmm_df = pd.DataFrame(response2['Items'])
        dmm_df['timestamp'] = dmm_df['timestamp'].astype("int64")
        rosa_df = dmm_df[dmm_df['timestamp'] > command_timestamp]
    exclude_prefixes = ['rosa .help', 'rosa .emic', 'rosa .table', 'rosa fc']

    pattern = r'^rosa\ \.[A-Za-z0-9_]*'
    rosa_df = rosa_df[~rosa_df['original_message'].str.contains(pattern, regex=True)]
    return rosa_df

new_df = pd.DataFrame(columns=["UUID", "address", "type", "error_return","original_message", "timestamp", "model"])

def clean_address(address):
    fuzzy_words = ["附近", "周圍", "旁邊", "靠近"]
    for word in fuzzy_words:
        address = address.replace(word, "")
    return address.strip()


def parse_info(df):
    MAX_RETRIES = 3  # 設定最大重試次數為3次

    for i, row in df.iterrows():
        original_message = row['original_message']
        timestamp = row['timestamp']
        messages = [
            {"role": "system", "content": f'''
            我們是專業的救災隊伍，而你是我們的訊息分析助手，
            你的任務是：協助我們提取訊息中最重要的三個資訊，分別是：1.地點 2.災難類型 3.災難嚴重程度。
            這三項是我們在救難派遣上最重要的訊息，因為有上述資訊才能決定我們欲派遣的資源與決定前往救援的路線。
            在災難類型的分類上，包含以下幾種：
            人道危機：人員死亡(A1)、人員受傷(A2-1:需要救援 ; A2-2:自行就醫)、人員受困(A3)。
            道路服務減損：交通阻斷(完全斷掉)(B1)、道路服務水準降低(B2)、無影響通行(B3)。
            建築結構損壞：建物倒塌(C1)、建物危險(C2)、建物輕微或無影響(C3)。
            民生服務中斷：停水(D1)、停電(D2)、無瓦斯、天然氣(D3)、無通訊服務或基地台損壞(D4)。
            洪水災害：淹水水深30公分以上、或者面積大於1000平方米為嚴重淹水(F1)、淹水情況低於F1則為輕微淹水(F2)。
            其他災害：嚴重影響人員活動(O1)、造成民眾不便(O2)。
            訊息諮詢：天氣諮詢(W1)、地震資訊詢問(W2)。
            此外，一個訊息可能包含多種災害類別(複合型災難)，
            例如：建物倒塌可能會伴隨人員受困或阻斷交通，若訊息中包含所有上述事件，該起災難在類型的歸類上為：A3, B1, C1。
            特別提醒1：路樹倒塌或其他路旁掉落物，除非伴隨人員傷亡或道路嚴重阻斷的訊息，否則皆判定為道路服務減損(B2)。
            特別提醒2：若道路紅綠燈與交通號誌故障，則為道路服務減損(B2)。
            特別提醒3：若道路發生淹水或積水，淹水情況為嚴重淹水(F1)，則也同時判定為交通阻斷(B1)；若為輕微淹水(F2)，則同時判定為道路服務減損(B2)。
            '''},

            {"role": "user", "content": f'''
            這是一個災難回報訊息的描述：\n{original_message}\n
            請嚴格遵守以下格式分析訊息及回覆。
            回覆時，回覆內容絕對不能超出災難回報訊息的描述。
            返回格式是：發生地點: xxx。 災難類型: yyy。
            地點請回傳可以被辨識的地點，避免回傳「附近」、「周圍」等模糊詞彙；災難類型請回傳SYSTEM Content的災害類別編號。
            注意！請勿回傳規定返回格式以外的字句。
            e.g.1： input: 羅斯福路四段1號路樹倒塌造成民眾受傷；output: 發生地點: 羅斯福路四段1號。災難類型: A2-2, B2。
            e.g.2： input: 台灣大學附近車禍阻斷交通，請問今天天氣如何？ output: 發生地點: 台灣大學。災難類型: B1, W1。
            e.g.3： input: 我住台東，我剛剛好像有感覺到地震，請問你有新的地震資訊嗎？ output: 發生地點: 台東。災難類型: W2。
            ########
            如果使用者提供訊息內無完整包含最重要的三個資訊：
            e.g.4： input: 人員受傷需救援，(其中沒有詳細的地址、地標、或沒有道路里程數)，output: 發生地點:None。災難類型:A2-1。訊息缺失: 發生地點。
            e.g.5： input: 辛亥路200號，(其中沒有災難類型)，output: 發生地點:辛亥路200號。災難類型:None。訊息缺失: 災難類型。
            e.g.5： input: rosa，output: 發生地點:None。災難類型:None。訊息缺失: 發生地點, 災難類型。
            ########
            如果在訊息中出現：測試、演練、測試資料等相關或同義的字詞，請當作真實訊息處理，一同協助完成程式測試與防災演練。
            '''}
        ]

        ##試試看聊天的類別>>扮演的角色(可能更嚴肅)

        info = None
        wait_time = 1  # 初始等待時間
        retries = 0  # 初始化重試次數為0

        while retries < MAX_RETRIES:
            try:
                chat_response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    temperature=0.2,
                    messages=messages,
                    max_tokens=100)
                info = chat_response['choices'][0]['message']['content'].strip()
                print(info)

                try:
                # 解析回應
                    address = info.split("發生地點: ")[1].split("。")[0].strip()
                    address = re.sub(r'(?i)rosa', '', address)
                    disaster_type = info.split("災難類型: ")[1].split("。")[0].strip().replace(" ", "")

                    # 檢查 "訊息缺失:" 是否存在於回應中
                    if "訊息缺失:" in info:
                        error_return = info.split("訊息缺失: ")[1].split("。")[0].strip()
                        if error_return.endswith("。"):
                            error_return = error_return[:-1]
                    else:
                        error_return = ""  # 或許你可以設定一個預設值或空字符串

                    if disaster_type.endswith("。"):
                        disaster_type = disaster_type[:-1]  # 移除結尾的句點

                except IndexError:
                    print("Unexpected format in OpenAI response, retrying...")
                    continue

                # 檢查災難類型是否只包含英文字母、數字和逗號
                if re.match("^[A-Za-z0-9,-]+$", disaster_type):
                    new_df.loc[i, 'UUID'] = row['UUID']
                    new_df.loc[i, 'address'] = address
                    new_df.loc[i, 'type'] = disaster_type
                    new_df.loc[i, 'error_return'] = error_return
                    new_df.loc[i, 'original_message'] = original_message
                    new_df.loc[i, 'timestamp'] = timestamp
                    new_df.loc[i, 'model'] = 'gpt-3.5-turbo'
                    break
            except openai.error.RateLimitError:
                print(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                time.sleep(wait_time)
                wait_time *= 2
            except openai.error.OpenAIError as e:
                print(f"OpenAI error: {str(e)}")
            finally:
                retries += 1 # 增加重試次數

        # 若重試次數已達到閾值，則設定資料為您提供的方式
        if retries == MAX_RETRIES:
            new_df.loc[i, 'UUID'] = row['UUID']
            new_df.loc[i, 'address'] = "None"
            new_df.loc[i, 'type'] = "None"
            new_df.loc[i, 'error_return'] = "系統繁忙，請稍後再試"
            new_df.loc[i, 'original_message'] = original_message
            new_df.loc[i, 'timestamp'] = timestamp
            new_df.loc[i, 'model'] = 'gpt-3.5-turbo'

    return new_df


#存入data_command_to_rosa中
def put_df_to_dynamodb(df, table_name, region_name, aws_access_key_id, aws_secret_access_key):
    dynamodb = boto3.resource('dynamodb',
                              region_name = region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

    table = dynamodb.Table(table_name)

    for i in range(len(df)):
        row = df.iloc[i].to_dict()
        for k, v in row.items():
            if pd.isnull(v):
                row[k] = ''
            elif k == 'timestamp':  # 如果鍵為'timestamp'，則轉換為 int
                row[k] = int(v)
            else:
                row[k] = str(v)
        table.put_item(Item=row)

def main():
    rosa_df = get_new_messages(region_name, aws_access_key_id, aws_secret_access_key)
    if rosa_df.empty == False:
        new_df = parse_info(rosa_df)
        put_df_to_dynamodb(new_df, "data_command_to_rosa", region_name, aws_access_key_id, aws_secret_access_key)
    print(datetime.now(pytz.utc).astimezone(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == "__main__":
    for i in range(6):
        main()
        time.sleep(0.1)