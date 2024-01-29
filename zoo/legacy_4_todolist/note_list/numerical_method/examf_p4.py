import datetime
from scipy.interpolate import CubicSpline, lagrange
from scipy.integrate import quad, simps, trapz
from numpy import linspace
from matplotlib.pyplot import plot, show, grid
import numpy as np
import matplotlib.pyplot as plt

datelist = ['2022-01-12','2022-01-27','2022-02-20','2022-02-28','2022-03-08','2022-03-16',
            '2022-04-03','2022-04-26','2022-05-02','2022-05-29','2022-06-05','2022-06-07',
            '2022-07-06','2022-07-24','2022-08-03','2022-08-22','2022-09-23','2022-09-29',
            '2022-10-04','2022-10-18','2022-11-28','2022-11-30','2022-12-03','2022-12-24']

templist =[11.8,26.4,
10.4,
28.5,
11,
30.8,
13.1,
34.6,
15.3,
34.5,
36.2,
20.3,
24.5,
39.4,
24.5,
38,
20.1,
35.3,
35.7,
18.7,
32.3,
17.2,
25,
7]

ydaylist =[]
for d, t in zip (datelist, templist):
    #convert d to datetime and calculate the day of the year
    d = datetime.datetime.strptime(d, '%Y-%m-%d')
    d = d.timetuple().tm_yday
    ydaylist.append(d)
    print (d, t)

f1 = CubicSpline(ydaylist, templist, bc_type='natural')
#f2 = lagrange(ydaylist, templist)

d1 = np.linspace(1, 365,2000)

f1temp = f1(d1)
#f2temp = f2(d1)
plt.plot(ydaylist, templist, 'o', label='data')    
plt.plot(d1, f1temp, 'x', label='f1')
#plt.plot(d1, f2temp, '*', label='f2')
plt.grid()
plt.show()
