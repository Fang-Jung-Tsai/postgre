from scipy.interpolate import CubicSpline
import numpy as np
import matplotlib.pyplot as plt

t = [0,   3,   5,   8,  13]
d = [0, 225, 383, 623, 993]
s = [75, 77,  80,  74,  72]

dis     = CubicSpline(t, d, bc_type='clamped')

speed   = CubicSpline(t, s, bc_type='clamped')

t1 = np.linspace(0, 15, 100)   

predict_dic = dis(t1)
predict_spd = speed(t1)

#distance, and speed and time = 10
print('distance:', dis(10), 'speed:', speed(10))

dis_n   = CubicSpline(t, d, bc_type='natural')
spe_n   = CubicSpline(t, s, bc_type='natural')

maxv1 = np.max(predict_spd)
maxv2 = np.max(spe_n(t1))

print('max speed:', maxv1, maxv2)
