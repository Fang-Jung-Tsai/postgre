#coding=utf-8

###########################################################################
# my Exception class
class TODEL (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TODEL(功能移除)'''
        return 

class TOFIX (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO FIX(有BUG)'''
        return 

class TODEV (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO DEV(發展中的功能)'''
        return
    
class TOADD (Exception):
    def __init__(self):
        self.msg = '''Exception Lily.ctao2_42 TO ADD(未來要加入的功能)'''
        return




# ###########################################################################
# #the following code is for testing only
# #it will not be executed when this module is imported

# def __test__current__code__1 ():
#     #check all attributes and methods of user_argument
#     conf = user_argument()

#     for key, value in conf.__dict__.items():
#         print(key, '=', value)

#     #chcek all attributes and methods of string
#     str1 = string()
#     print (str1.to_nsgkey() )
#     print (str1.to_nsgkey(1) )
#     print (str1.to_nsgkey(2) )
#     print (string.alnum('G:\\NCREE_GIS\\streetblock.sqlite') )
#     print (string.alnum_uuid() )

#     #check all attributes and methods of webapi
#     #if conf has cwb_key, then use it, else use the default value
#     url = 'www.google.com'
   
#     if hasattr(conf, 'cwb_key'):
#         url = f'https://opendata.cwb.gov.tw/api/v1/rest/datastore/F-D0047-061?Authorization={conf.cwb_key}&format=JSON&sort=time'
        
#     web = webapi(url)
#     print(web.response.status_code)
#     print(web.type) 

#     if web.json is not None:
#         print(web.json)

#     #check all attributes and methods of answer
#     ans = answer()
#     print(ans.tick())   
#     print(ans.tick())
#     lst1 = [1,2,3,4,5,6,7,8,9,10,11,12,13]

#     new_list = ans.numbering_list(lst1)

#     print('nparts',ans.split_list_to_n_parts(lst1, 4))
#     print('chunks',ans.split_list_to_chunks(lst1, 5))

#     print(new_list)

# def __test__current__code():
#     this_path = user_argument.get_last_folder()
#     print(this_path)

# if __name__ == '__main__':
#     #__test__current__code__1()  
#     argu = user_argument()
#     #clear console
#     import os
    
#     #print all attributes and methods of user_argument
#     for key, value in argu.__dict__.items():
#         print(key, '=', value)

