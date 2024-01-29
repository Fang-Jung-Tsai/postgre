import requests

class  line_notify:
    def __init__ (self, linenotify_key):
        # LINE Notify 權杖
        self.TOKEN_NOTIFY = linenotify_key

    def send_text(self, message):

        rd = requests.post(
            f"https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {self.TOKEN_NOTIFY}"},
            data={"message": f"{message}"})

        return rd

    def send_image(self, image_file_path, message=''):

        # 要發送的訊息

        # HTTP 標頭參數與資料
        headers = { "Authorization": "Bearer " + self.TOKEN_NOTIFY }
        data = { 'message': message }

        # 要傳送的圖片檔案
        image = open(image_file_path, 'rb')
        files = { 'imageFile': image }

        # 以 requests 發送 POST 請求
        rd= requests.post("https://notify-api.line.me/api/notify", headers = headers, data = data, files = files)

        return rd 
## 可獨立執行
if __name__ == '__main__':
    # from postgis_CE13058 import postgis_CE13058 as postgis
    # db = postgis()
    # df = db.read_data ('data_rosa_ctyang_line_notify')

    # line_notify_dict = {}
    # for index, row in df.iterrows():
    #     line_notify_dict[row[0]] = line_notify(row[0])
        
    
    # for key, value in line_notify_dict.items():
    #     print(key, value)
    #     value.send_text('作業測試')
  
    l = line_notify("lprdrf5y26zIgLKA65UsyFEVBStLqiAckC35H08JEtn")
    rd = l.send_text("test fj")
    print(rd)