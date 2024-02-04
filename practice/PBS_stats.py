import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.table as tbl
import matplotlib as mpl
from matplotlib.font_manager import fontManager
from argparse import ArgumentParser

import sys
my_package_path = os.path.expanduser("~/postgre/zoo")
# Add the path to sys.path
sys.path.append(my_package_path)

from postgis_CE13058 import postgis_CE13058

current_file_directory = os.path.dirname(os.path.abspath(__file__))

history_table = pd.DataFrame()

def format_datetime(datetime_string):
    # column datetime format 2024-01-01 06:12:52
    converted_datetime = datetime.strptime(datetime_string, "%Y/%m/%d-%H:%M:%S")
    
    return converted_datetime

def format_datetime_v2(datetime_string):
    # column moddttm format 2024-01-01 06:12:52:041
    converted_datetime = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S.%f")
    
    return converted_datetime

def custom_filter(row, decreased_datetime):
    converted_datetime = row['datetime']
    return converted_datetime > decreased_datetime

def count_road_type(filtered_df, key='areaname', condition=''):
    global history_table
    roadtype_stats = {'事故': 0, '交通管制': 0, '交通障礙': 0, '號誌故障': 0, '道路施工': 0, '阻塞': 0, '災變': 0, '其他': 0}

    for id, row in filtered_df.iterrows():
        uid = row['uid']
        history = history_table[history_table['uid']==uid]

        if condition == '台北市':
            cond1 = history['x1'] == 121.5598
            cond2 = history['y1'] == 25.09108
            history = history[cond1 & cond2]
        elif condition != '全台' and len(condition) > 0:
            history = history[history[key] == condition] 
        
        if len(history):
            history = history.sort_values(by="moddttm", ascending=True)
            
            roadtype = history.roadtype.unique()

            try:
                if len(roadtype) > 1:
                    for t in roadtype:
                        roadtype_stats[t] += 1
                else:
                    roadtype_stats[roadtype[0]] += 1

            except Exception as e:
                print(e)

    return roadtype_stats

def plot_stats_table(filtered_df:pd.DataFrame, begindate=datetime.now(), enddate=datetime.now()):

    fontManager.addfont(os.path.expanduser("~/postgre/practice/style/taipei_sans_tc_beta.ttf"))
    mpl.rc('font', family='Taipei Sans TC Beta')

    row_lbls = ['全台', '台北市',
    '中山高速公路-國道１號',  '汐止楊梅-國道1號高架', '福爾摩沙高速公路-國道３號','蔣渭水高速公路-國道５號',
    '機場支線-國道２號', '台中環線-國道４號', '國道６號-國道６號', '台南支線-國道８號']
    col_lbls = ['事故', '交通管制', '交通障礙', '號誌故障', '道路施工', '阻塞', '災變',  '其他']

    data = []
    for row_lbl in row_lbls:
        roadtype_stats = count_road_type(filtered_df, condition=row_lbl)
        data.append(list(roadtype_stats.values()))

    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(10, 3))

    # Hide the axes
    ax.axis('off')

    # Create a table and add it to the figure
    table = ax.table(cellText = data, rowLabels = row_lbls,  colLabels = col_lbls, 
                    rowColours =["lightgray"] * len(row_lbls), colColours =["lightgray"] * len(col_lbls), 
                    loc='upper center', cellLoc='center', colWidths=[0.15] * len(col_lbls))

    # Customize the appearance of the table
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2,1.2)

    # Add the table to the axis
    # ax.add_table(table)

    ax.set_title(f'{datetime.strftime(begindate, "%Y-%m-%d")} 即時路況統計',
                fontweight ="bold", fontsize=16) 

    try:
        # save the plot
        fig_fn = os.path.join(current_file_directory, 'figures', f'{datetime.strftime(begindate, "%Y-%m-%d")}.png')
        plt.tight_layout()
        plt.savefig(fig_fn)

        return fig_fn
    except Exception as e:
        raise e

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--days", default=1, help="how many days before today")
    args = parser.parse_args()

    postgis = postgis_CE13058()
    history_table = postgis.read_data('data_rosa_fj_pbs')
    key_table = postgis.read_data('data_rosa_fj_pbs_key')
    history_table = history_table.fillna("")
    key_table['datetime'] = key_table['datetime'].map(format_datetime)  

    current_datetime = datetime.now()
    one_day = timedelta(days=int(args.days))
    decreased_datetime = current_datetime - one_day

    filtered_df = key_table[key_table.apply(custom_filter, args=(decreased_datetime, ), axis=1)]

    try:
        fig_fn = plot_stats_table(filtered_df, begindate=decreased_datetime, enddate=current_datetime)

        from webapi_line_notify import line_notify
        from baseobject import baseobject

        #get essential attributes from rosa_config.json
        argu = baseobject()

        tokens = [argu.LINE_ROSA_GROUP] # , argu.LINE_ROSA_THEATRE

        for token in tokens:
            l = line_notify(token)
            rd = l.send_image(fig_fn, message = '警廣即時路況')

    except Exception as e:
        print(e)