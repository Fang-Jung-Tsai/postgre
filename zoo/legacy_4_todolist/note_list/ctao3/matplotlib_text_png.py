import os
import matplotlib.pyplot as plt

#FOR BIG5 FONT
from matplotlib.font_manager import FontProperties
from core import user_argument as configuration
from core import string, answer

class text_png :

    def __init__ (self):
        self.conf   = configuration()
        self.font = FontProperties(fname=self.conf.font_cp950, size=12)
        self.font_step = 12 + 2 # 12 is font size, 2 is buffer 

    def writelines(self, strlist, filneame=None):
        
        if len(strlist) == 0:
            return None
        
        # determine width based on max length of strlist
        bilist = [ sz.encode('utf-8') for sz in strlist]
        wnum   =  max( [len(sz) for sz in bilist])
        width   = wnum*self.font_step/100 + 1

        # determine hieight based on length of strlist
        hnum = len(strlist)
        hieight = hnum*self.font_step/100 + 1 

        self.fig    = plt.figure(figsize=( width , hieight))
        self.ax     = self.fig.add_subplot()
        self.ax.axis([0, wnum, 0, hnum])
        self.ax.axis('off')

        y = hnum - 0.1
        for text in strlist:
            self.ax.text(1, y, text, fontproperties=self.font)
            y=y-1
        #
        if filneame is None:
            filename = os.path.join(self.conf.factory, f'ctao3.text_png.{string.alnum_uuid()}.png' )

        plt.savefig( filename , bbox_inches='tight')
        #plt.show()
        return filename

if __name__ =='__main__':
    ans = answer()
    png = text_png()

    lines=['ROSA LineBOT1',
            'ROSA LineBOT2',
            '1料擷取時間:2023-02-18 05:42',
            '2政府開放資料 6069 中央氣象局 天氣預報',
            '3料擷取時間:2023-02-18 05:42',
            '4政府開放資料 6069 中央氣象局 天氣預報',
            '5料擷取時間:2023-02-18 05:42',
            '6政府開放資料 6069 中央氣象局 天氣預報',
            '7料擷取時間:2023-02-18 05:42',
            '8政府開放資料 6069 中央氣象局 天氣預報',
            '9資料區間 2023-02-18 18:00~2023-02-19 06:00 12345678901234567890',
            '8資料區間 2023-02-18 18:00~2023-02-19 06:00 12345678901234567890',
            '7資料區間 2023-02-18 18:00~2023-02-19 06:00 12345678901234567890',
            '1資料區間 2023-02-18 18:00~2023-02-19 06:00 12345678901234567890']

    filename = png.writelines(lines)
    