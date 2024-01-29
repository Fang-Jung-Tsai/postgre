import win32wnet

# 定義網路共享目錄路徑
share_path = r'\\ssc'

# 定義網域帳號資訊
username = 'NCREE\\ctyang'
password = 'ncree..2023.Vm'
domain = 'NCREE'

# 登入網域帳號
try:


    win32wnet.WNetAddConnection2(
        win32wnet.NETRESOURCE(),
        password,
        username,
        0,
    )
    print("登入成功")
except Exception as e:
    print("登入失敗:", str(e))

# 讀取共享目錄中的檔案
try:
    with open(share_path + r'\file.txt', 'r') as file:
        content = file.read()
        print("檔案內容:", content)
except Exception as e:
    print("讀取檔案失敗:", str(e))

# 登出網域帳號
try:
    win32wnet.WNetCancelConnection2(share_path, 0, 0)
    print("登出成功")
except Exception as e:
    print("登出失敗:", str(e))
