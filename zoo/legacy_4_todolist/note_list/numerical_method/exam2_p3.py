import numpy as np
from scipy.interpolate import CubicSpline, lagrange
from scipy.integrate import quad, simps, trapz

x = np.array([1.8, 2.0, 2.2, 2.4, 2.6])
y = np.array([3.12014, 4.42569, 6.04241, 8.03014, 10.46675  ])

f1 = CubicSpline(x, y, bc_type='clamped')
f2 = lagrange(x, y)

a = 1.8
b = 2.6
n = 100

x_new = np.linspace(a, b, n)
f1_value = f1(x_new)
f2_value = f2(x_new)
result1 = simps(f1_value, x_new)
result2 = simps(f1_value, x_new)
result3, error1 = quad(f1, a, b)
result4 = simps(f2_value, x_new)
result5 = trapz(f2_value, x_new)
result6, error2 = quad(f2, a, b)

print('result1:', result1, 'result2:', result2, 'result3:', result3, 'result4:', result4, 'result5:', result5, 'result6:', result6)

