from scipy.interpolate import CubicSpline
import numpy as np
import matplotlib.pyplot as plt

d  = [0, 6, 10, 13, 17, 20, 28]
fs = [6.67, 17.33, 42.67, 37.33, 30.10, 29.31, 28.74]
ss = [6.67, 16.11, 18.89, 15.00, 10.56,  9.44,  8.89]

f1 = CubicSpline(d, fs, bc_type='natural')
f2 = CubicSpline(d, ss, bc_type='natural')

x1 = np.linspace(0, 30, 100)   
y1 = f1(x1)
y2 = f2(x1)

plt.plot(x1, y1, label='f1')
plt.plot(x1, y2, label='f2')
plt.grid()
plt.show()

max1= np.max(y1)
max2= np.max(y2)
print('max1:', max1, 'max2:', max2)
