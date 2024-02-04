import feedparser
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
my_package_path =  os.path.expanduser("~//home/fj/postgre/zoo")
# Add the path to sys.path
sys.path.append(my_package_path)

from postgis_CE13058 import postgis_CE13058
from webapi_line_notify import line_notify
from baseobject import baseobject

#WSJ
feed_url = 'https://feeds.a.dj.com/rss/RSSWorldNews.xml'

def format_datetime(datetime_string):
    converted_datetime = datetime.strptime(datetime_string, "%a, %d %b %Y %H:%M:%S %z")
    formatted_datetime = converted_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    return formatted_datetime


def custom_filter(row, decreased_datetime):
    converted_datetime = datetime.strptime(row['published'], "%Y-%m-%d %H:%M:%S")
    return converted_datetime > decreased_datetime

if __name__ == '__main__':

    #get essential attributes from rosa_config.json
    argu = baseobject()

    tokens = [argu.LINE_ROSA_GROUP, argu.LINE_ROSA_THEATRE]
    # 解析饋送
    feed = feedparser.parse(feed_url)

    df = pd.DataFrame(feed.entries)
    df = df[['title', 'link', 'summary', 'published']]
    df['published'] = df['published'].map(format_datetime)    

    postgis = postgis_CE13058()

    current_datetime = datetime.now()
    one_day = timedelta(days=1)
    decreased_datetime = current_datetime - one_day
    
    filtered_df = df[df.apply(custom_filter, args=(decreased_datetime, ), axis=1)]

    old_news = postgis.read_data('data_rosa_fj_wsj_news')
    if len(old_news):
        postgis.append_data(filtered_df, 'data_rosa_fj_wsj_news')
    else:
        postgis.write_data(filtered_df, 'data_rosa_fj_wsj_news')

    msgs = []
    for i, row in filtered_df.iterrows():
        msg = f'標題 {i+1}: {row["title"]}\n時間: {row["published"]}\n連結: {row["link"]}'
        msgs.append(msg)
    
    msgs = '[今日 WSJ]\n' + '\n\n'.join(msgs)

    for token in tokens:
        l = line_notify(token)
        rd = l.send_text(msgs)
    