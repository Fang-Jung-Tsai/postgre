#coding=utf-8
import os
import sys
import sqlite3
import hashlib
import multiprocessing
from functools import partial
import datetime
import pandas

def process_object(obj, func):
    return func(obj)

class File:
    def __init__(self, file_path):
        self.file_path = file_path
        self.created_time = None
        self.modified_time = None
        self.file_size = None
        self.file_extension = None
        self.file_base_name = None
        self.hash = None

    def get_file_info(self):
        try:
            # Get the created timestamp of the file
            created_timestamp = os.path.getctime(self.file_path)
            self.created_time = datetime.datetime.fromtimestamp(created_timestamp)

            # Get the modified timestamp of the file
            modified_timestamp = os.path.getmtime(self.file_path)
            self.modified_time = datetime.datetime.fromtimestamp(modified_timestamp)

            # Get the file size in bytes
            self.file_size = os.path.getsize(self.file_path)

            # Extract the file extension and base name
            file_name = os.path.basename(self.file_path)
            file_name_parts = os.path.splitext(file_name)
            self.file_extension = file_name_parts[1]
            self.file_base_name = file_name_parts[0]

        except FileNotFoundError:
            print(f'{self.file_path} not found')

        except IOError:
            print(f'unable to access {self.file_path}')

        return self

    def calculate_hash(self):
        # Calculate the SHA-256 hash
        hash_algorithm = hashlib.sha256()

        try:
            with open(self.file_path, 'rb') as file:
                for chunk in iter(lambda: file.read(4096), b''):
                    hash_algorithm.update(chunk)

            self.hash = hash_algorithm.hexdigest()

        except FileNotFoundError:
            print(f'{self.file_path} not found')
        except IOError:
            print(f'unable to open {self.file_path} ')

        return self

    @staticmethod
    def calculate_directory_files_info(directory_path):
        filelist = []
        for root, directories, files in os.walk(directory_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                filelist.append(file_path)

        num_cores = min(multiprocessing.cpu_count(), 12)
        pool = multiprocessing.Pool(processes=num_cores)

        file_objects = [File(file_path) for file_path in filelist]

        hash_func = partial(process_object, func=File.calculate_hash)
        info_func = partial(process_object, func=File.get_file_info)

        file_objects = pool.map(hash_func, file_objects)
        file_objects = pool.map(info_func, file_objects)

        res_list = []
        for file_obj in file_objects:
            res_list.append(file_obj.__dict__)

        res_pd = pandas.DataFrame(res_list)
        res_pd = res_pd.sort_values(by=['file_size', 'hash'])

        msword_pd = res_pd[res_pd['file_extension'].isin(['.doc', '.docx'])]
        mspptx_pd = res_pd[res_pd['file_extension'].isin(['.ppt', '.pptx'])]

        dup_pd = res_pd[res_pd.duplicated(subset=['hash', 'file_size'], keep=False)]
        dup_pd = dup_pd.dropna(subset=['hash'])
        dup_pd = dup_pd[dup_pd['file_size'] != 0.0]

        return {'res': res_pd, 'dup': dup_pd, 'msword': msword_pd, 'mspptx': mspptx_pd}

    @staticmethod
    def get_target_db(directory_path):
        user_dir = os.path.expanduser("~")
        download_dir = os.path.join(user_dir, r'Downloads')
        current_date = datetime.datetime.now()
        date_string = current_date.strftime("%Y%m%d")
        target_db = f'''{download_dir}/{date_string}_{os.path.basename(directory_path)}.sqlite'''
        return target_db


if __name__ == '__main__':
    directory_path = 'g:\\'

    dictionary = File.calculate_directory_files_info(directory_path)
    target_db = File.get_target_db("gdriver")

    conn = sqlite3.connect(target_db)

    for table_name, dataframe in dictionary.items():
        dataframe.to_sql(f'calc_file_{table_name}', conn, index=False, if_exists='replace')

    conn.close()
