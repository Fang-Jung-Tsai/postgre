import smbclient
import shutil

class share_folder:

    def __init__(self, server, user, password):
        self.server = server
        self.user = user
        self.password = password
        self.client = smbclient
    
    def connect(self):
        self.client.ClientConfig(username=self.user, password=self.password)

    def get_file(self, remote_path, local_path):
        if self.client.path.exists(remote_path):
            with self.client.open_file(remote_path, mode='rb') as f:
                with open(local_path, 'wb') as l:
                    shutil.copyfileobj(f, l)

    def list_dir(self, remote_path):
        return self.client.listdir(remote_path)
    
    def get_latest_file_time(self, remote_path):
        latest_time = 0
        fn = ''
        for file in self.list_dir(remote_path):
            file_time = self.client.path.getmtime(remote_path + '\\' + file)
            if file_time > latest_time:
                latest_time = file_time
                fn = file
        return fn, latest_time
    
    def walk(self, remote_path):
        return self.client.walk(remote_path)
    

# if __name__ == '__main__':

#     teles = TelesConnection(server, username, password)
#     teles.connect()
#     print(teles.list_dir(rf'\\{server}\{share}'))
    # latest_file = teles.get_latest_file_time(rf'\\{server}\{share}')
    # print(latest_file)
    # # download latest file
    # teles.get_file(rf'\\{server}\{share}\{latest_file[0]}', fr'C:\Users\jitin\Desktop\myscript\BLOSSOM_RESQ_NAVIGATOR\{latest_file[0]}')
    