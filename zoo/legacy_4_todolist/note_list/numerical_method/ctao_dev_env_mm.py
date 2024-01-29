#Here is a Python program that provides two options: installing and uninstalling 
#the matplotlib, numpy, and scipy modules using the pip module. 
#```python
#```
#matplotlib     是一個用於繪製二維圖形的 Python 库。
#numpy          是一個用於處理多維數組和矩陣運算的 Python 库。
#scipy          是一個用於科學計算和技術計算的 Python 库，它提供了許多高級數學函數和算法。

import pip
#modules = ['matplotlib', 'numpy', 'scipy', 'pandas']
modules = ['openai']

def install():

    for module in modules:
        pip.main(['install', module])

def uninstall():
    for module in modules:
        pip.main(['uninstall', '-y', module])

option = input('Enter 1 to install or 2 to uninstall: ')
if option == '1':
    install()
elif option == '2':
    uninstall()
else:
    print('Invalid option')
