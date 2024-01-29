import numpy as np

ang1 = np.pi/4
ang2 = np.pi/6
ang3 = np.pi/2

Rz1=np.array([[np.cos(ang1), -np.sin(ang1), 0], 
              [np.sin(ang1), np.cos(ang1), 0], 
              [0, 0, 1]])

Rx = np.array([[1, 0, 0],
               [0,np.cos(ang2), -np.sin(ang2)], 
               [0,np.sin(ang2), np.cos(ang2)]])

Rz2=np.array([[np.cos(ang3), -np.sin(ang3), 0],
                [np.sin(ang3), np.cos(ang3), 0],
                [0, 0, 1]])

array1 = [1,0,0]
array2 = [0,1,0]
array3 = [0,0,1]
array4 = [1,1,1]
array5 = [-1,0,1]

print ('v1:', Rz1@Rx@Rz2@array1)
print ('v2:', Rz1@Rx@Rz2@array2)
print ('v3:', Rz1@Rx@Rz2@array3)
print ('v4:', Rz1@Rx@Rz2@array4)
print ('v5:', Rz1@Rx@Rz2@array5)
