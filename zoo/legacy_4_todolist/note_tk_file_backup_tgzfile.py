import tkinter as tk
from tkinter import filedialog
#import shutil
import os
import tarfile
import gzip
from datetime import datetime

class BackupApp:
    def __init__(self, master):
        self.master = master
        master.title("檔案備份程式")

        # 選擇目錄按鈕
        self.button1 = tk.Button(master, text="選擇來源目錄", command=self.choose_source_dir)
        self.button1.pack(side="top", fill="x", padx=5, pady=5)

        self.button2 = tk.Button(master, text="選擇目標目錄", command=self.choose_dest_dir)
        self.button2.pack(side="top", fill="x", padx=5, pady=5)

        self.button3 = tk.Button(master, text="開始備份", command=self.backup)
        self.button3.pack(side="top", fill="x", padx=5, pady=5)

        # 標籤
        self.label1 = tk.Label(master, text="來源目錄:")
        self.label1.pack(side="top", padx=5, pady=5)

        self.source_dir = tk.StringVar()
        self.source_dir_label = tk.Label(master, textvariable=self.source_dir)
        self.source_dir_label.pack(side="top", padx=5, pady=5)

        self.label2 = tk.Label(master, text="目標目錄:")
        self.label2.pack(side="top", padx=5, pady=5)

        self.dest_dir = tk.StringVar()
        self.dest_dir_label = tk.Label(master, textvariable=self.dest_dir)
        self.dest_dir_label.pack(side="top", padx=5, pady=5)

        self.label3 = tk.Label(master, text="訊息視窗:")
        self.label3.pack(side="top", padx=5, pady=5)

        # 訊息視窗
        self.text = tk.Text(master, height=10, width=50)
        self.text.pack(side="top", padx=5, pady=5)

    def choose_source_dir(self):
        self.source_dir.set(filedialog.askdirectory())
        self.source_dir_label.config(fg="green", text=self.source_dir.get())

    def choose_dest_dir(self):
        self.dest_dir.set(filedialog.askdirectory())
        self.dest_dir_label.config(fg="green", text=self.dest_dir.get())

    def backup(self):
        try:
            src = self.source_dir.get()
            tar = self.dest_dir.get()
            
            self.compress_dir_to_tgz(self.source_dir.get(), self.dest_dir.get())
            self.text.insert(tk.END, "備份成功！\n")
        except Exception as e:
            self.text.insert(tk.END, f"備份失敗：{str(e)}\n")

    def compress_dir_to_tgz(self, source_dir, target_dir):
        """
        This function compresses a source directory into a tar+gzip file and
        saves it to a target directory. The filename of the compressed file
        is the name of the source directory + today's date + ".tgz" extension.
        """
        # Get the name of the source directory
        source_dir_name = os.path.basename(source_dir)

        # Get today's date in the format YYYY-MM-DD
        today = datetime.today().strftime('%Y%m%d_%H%M')

        # Define the name of the compressed file
        compressed_filename = f"{source_dir_name}_{today}.tgz"

        # Set the full path of the compressed file
        compressed_file_path = os.path.join(target_dir, compressed_filename)

        # Create the tarfile object
        with tarfile.open(compressed_file_path, "w:gz") as tar:
            # Loop over each file in the source directory
            for file in os.listdir(source_dir):

                # Set the full path of the file
                file_path = os.path.join(source_dir, file)

                # Try to add the file to the tarfile object
                try:
                    tar.add(file_path, arcname=os.path.join(source_dir_name, file))
                except Exception as e:
                    errfile.append(f"Skipping file: {file_path} - {e}")            

root = tk.Tk()
app = BackupApp(root)
root.mainloop()


