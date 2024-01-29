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

    def send_image(self, image_file_path):

        # 要發送的訊息
        message = '(pylily)發送的訊息與圖片'

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
    from core import configuration
    conf = configuration()
    
    key = conf.ROSAL_linenotify_DEV2

    lobj = line_notify(key)
    lobj.send_text('programmer test')
