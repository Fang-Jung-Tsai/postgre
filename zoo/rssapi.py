import feedparser
import pandas as pd
from postgis_CE13058 import *
from webapi_line_notify import line_notify

# 指定 RSS 饋送的 URL
# feed_url = 'https://about.pts.org.tw/rss/XML/newsletter.xml'

#WSJ
feed_url = 'https://feeds.a.dj.com/rss/RSSWSJD.xml'

#PeoPo 公民新聞
#https://www.peopo.org/peopo_agg/feed?post_u=1381

				
# Title		        Copy URLs to RSS Reader		 
# Top Stories		http://rss.cnn.com/rss/cnn_topstories.rss
# World		        http://rss.cnn.com/rss/cnn_world.rss		
# U.S.		        http://rss.cnn.com/rss/cnn_us.rss		
# Business (CNNMoney.com)		http://rss.cnn.com/rss/money_latest.rss		
# Politics		    http://rss.cnn.com/rss/cnn_allpolitics.rss		
# Technology		http://rss.cnn.com/rss/cnn_tech.rss	
# Health		    http://rss.cnn.com/rss/cnn_health.rss		
# Entertainment		http://rss.cnn.com/rss/cnn_showbiz.rss		
# Travel		    http://rss.cnn.com/rss/cnn_travel.rss		
# Video		        http://rss.cnn.com/rss/cnn_freevideo.rss		
# CNN 10		    http://rss.cnn.com/services/podcasting/cnn10/rss.xml		
# Most Recent		http://rss.cnn.com/rss/cnn_latest.rss		
# CNN Underscored	http://rss.cnn.com/cnn-underscored.rss		



# 解析饋送
feed = feedparser.parse(feed_url)

df = pd.DataFrame(feed.entries)
df = df[['title', 'link', 'summary', 'published']]
print(df.head)

# postgis = postgis_CE13058()

# postgis.drop_table('data_rosa_fj_news_0118')
# postgis.write_data(df, 'data_rosa_fj_news_0118')