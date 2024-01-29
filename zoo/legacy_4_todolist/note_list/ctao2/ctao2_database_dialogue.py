#//# coding=utf-8
import pandas
import os, socket, platform, datetime
from Lily.ctao2.ctao2_database_alias import alias as mtable 

from tkinter import * 

class askopenfilename:

    def __init__(self, nametext, argument_list =[ ['source', 'xlsx'], 
                                                  ['target', 'sqlite'], 
                                                  ['arg1',  None], 
                                                  ['arg2',  None] ] ) :

        self.label    = {}
        self.entry    = {}
        self.button   = {}
        self.values   = {}

        self.arg_num        = len(argument_list)         
        self.dirname        = os.getcwd()
        self.mainframe      = Tk()
        self.mainframe.title(nametext)
       
        for vars, ind in zip(argument_list, range(self.arg_num)) :
            arg_name = vars[0]
            arg_type = vars[1]

            self.label[arg_name] = Label(self.mainframe, text = arg_name )
            self.entry[arg_name] = Entry(self.mainframe, width = 50)
            self.label[arg_name].grid ( row = ind, column = 0, stick = W, padx = 2, pady = 2) 
            self.entry[arg_name].grid ( row = ind, column = 1, columnspan = 3, stick = W, padx = 2, pady = 2)
            
            if arg_type is not None: 
                fun = lambda name = arg_name, type= arg_type, ind=ind: self.button_fun (name, type, ind) 
                self.button[arg_name] = Button (self.mainframe, text = arg_type , command =  fun)
                self.button[arg_name].grid (row = ind, column = 4, stick = 'nesw', padx = 2, pady = 2)
            else:
                self.button[arg_name] = None

        self.button_ok        = Button(self.mainframe, text = 'ok', command = self.button_fun_ok )
        self.button_ok.grid   (row = self.arg_num , column = 4, stick = 'nesw', padx = 2, pady = 2) 

        self.mainframe.mainloop()

    def button_fun(self, argname, argtype, ind):
        from tkinter.filedialog import askopenfilename

        filename = askopenfilename(initialdir = self.dirname,
                       filetypes =( ('choose a File',   '*.{0}'.format(argtype) ),   ("All Files","*.*")),
                       title = 'Choose a file.')

        self.entry[argname].delete(0, END)
        self.entry[argname].insert(0, filename)


    def button_fun_ok(self):

        for key in self.entry :
            self.values[key] =  self.entry[key].get()
        
        self.mainframe.destroy() 

class IORedirector(object):
    '''A general class for redirecting I/O to this Text widget.'''
    def __init__(self, text_area):
        self.text_area = text_area
        self.line = 0
class StdoutRedirector(IORedirector):
    '''A class for redirecting stdout to this Text widget.'''
    def write(self, str):
        
        self.text_area.insert(self.text_area.END, str)
        self.line = self.line + 1

class dialogue_table(askopenfilename):

    def __init__(self, dialoguebox_title = 'choose sqlite database path') :
        super().__init__(dialoguebox_title, [['database_path' , 'sqlite']])
        
    def button_fun_ok(self):
        from Lily.ctao2.ctao2_database_mediator import manidb as database

        self.database_path = self.entry['database_path'].get() 
        self.mydb = database( self.database_path )
        self.tab_list  = sorted(self.mydb.tables().index.values)
        arg_num   = len(self.tab_list)

        if hasattr(self, 'button_ok'):
            self.button_ok.grid_forget()

        if hasattr(self, 'button'):
            for ind in self.button:
                self.button[ind].grid_forget()

        self.button_pickup        = Button(self.mainframe, text = 'pick up a table', bg ='red' , command = self.pick )
        self.button_pickup.grid   (row = 3 , column = 0, stick = 'nesw', padx = 2, pady = 2) 

        self.listbox = Listbox(self.mainframe, width = 64)

        for key, tab in zip ( range(arg_num), self.tab_list ):
            self.listbox.insert(key, tab)

        self.listbox.grid(row=4, column=0, columnspan = 2  )

        self.text_box = Text(self.mainframe, wrap='word', height = 11, width=50)
        self.text_box.grid(row=4, column=3, columnspan = 2, sticky='NSWE', padx=5, pady=5)
        #sys.stdout = StdoutRedirector(self.text_box)


    def pick(self):
        #self.table_name = self.radiovalue.get()
        values = [self.listbox.get(idx) for idx in self.listbox.curselection()]
       
        self.table_name =   values[0]  if len(values) == 1 else 'none'

        print (self.table_name)
        #self.mainframe.destroy() 

class table(mtable):

    def __init__(self, message_to_user='choose sqlite database path' ) :
        ui = dialogue_table(message_to_user)
        super().__init__(ui.database_path, ui.table_name) 

if __name__ == '__console__' or __name__ == '__main__' :
    import Lily.ctao2.ctao2_hostmetadata as chmd
    hobj1= chmd.hostmetadata()
    print ('check moudel Lily.ctao.hostmetadata')
    print (hobj1.callname, hobj1.hostname, hobj1.platform)
    print (hobj1.warehouse, hobj1.factory)

    if hobj1.platform[:7] =='Windows': 
        tab1 = table('pick up a table')
        df = tab1.read()
        tab1.timer()
        print (df)


