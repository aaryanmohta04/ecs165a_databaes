from lstore import bufferpool
from lstore.table import Table
from lstore.bufferpool import *
import csv
import os
import pickle
import array, struct

TABLEKEY = 0
TABLENUMCOL = 1
TABLECURPG = 2
TABLECURBP = 3
TABLECURREC = 4

class Database():

    def __init__(self):
        self.tables = []
        self.tablenames = []
        #self.tableDirectory = []
        self.bufferpool = None
        self.path = ''
        pass

    # Not required for milestone1
    def open(self, path):
        self.bufferpool = Bufferpool(path) #create a bufferpool object for this db
        self.bufferpool.start_db_dir() #initiate directory given the path (or check if it exists)
        self.path = path
        # tables_path = path + '/tables/tables.csv'
        
        # if os.path.exists(tables_path):
        #     with open(tables_path, 'r') as file:
        #         csvreader = csv.reader(file)
        #         tableNames = []
        #         tableNames = [row for row in csvreader]
        #         if(tableNames != []):
        #             for tablename in tableNames[0]: 
        #                 self.tablenames.append(tablename)       

        if not os.path.exists(self.path):
            os.mkdir(path)
            os.mkdir(path + "/tables")
        

        # pull all tables, update metadata
        path = f"{self.path}/tables"
        for entry in os.listdir(path):
            specifictablepath = os.path.join(path, entry)
            if os.path.isdir(specifictablepath):
                with open(specifictablepath + "/metadata.bin", 'rb') as file:
                    binary_data = file.read()
                arr = array.array('i', struct.unpack('i'* (len(binary_data)//struct.calcsize('i')), binary_data))
                tableName = entry
                num_columns = arr[TABLENUMCOL]
                table_key = arr[TABLEKEY]
                self.bufferpool.start_table_dir(tableName, num_columns)
                table = Table(tableName,num_columns , table_key, self.bufferpool,False )
                table.curPageRange = arr[TABLECURPG]
                table.curBP = arr[TABLECURBP]
                table.curRecord = arr[TABLECURREC]
                self.tables.append(table)
                # self.tablenames.append(name)


        # create indices, page directories for all columns. 
        for table in self.tables:
            if os.path.exists(path + f"/tables/{table.name}/indices.pkl"):
                table.index.load_index(path + f"/tables/{table.name}/indices.pkl")
            if os.path.exists(path + f"/tables/{table.name}/pagedirectory.pkl"):
                with open(path + f"/tables/{table.name}/pagedirectory.pkl", 'rb') as file:
                    self.table.page_directory = pickle.load(file)

        # pulls page ranges and updates the table. Actual pagerange implementantion left
        for table in self.tables: 
            path = f"{self.path}/tables/{table.name}"
            table.pullpagerangesfromdisk(path)
                         

        pass
    
    def close(self):

        # close and save indexes for all tables: 
        for table in self.tables:
            picklepath = self.path + f"/tables/{table.name}/indices.pkl"
            table.index.close_and_save(picklepath)
            directorypath = self.path + f"/tables/{table.name}/pagedirectory.pkl"
            with open(directorypath, 'wb') as file:
                pickle.dump(table.page_directory, file)
            metadatapath = self.path + f"/tables/{table.name}/metadata.bin"
            table.savemetadata(metadatapath)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
       
        self.bufferpool.start_table_dir(name, num_columns) #makes table directory
        #self.bufferpool.allocate_page_range(num_columns, 0)
        table = Table(name, num_columns, key_index, self.bufferpool, True)
        if not os.path.exists(self.path + f"/tables/{table.name}"):
            os.mkdir(self.path + f"/tables/{table.name}")
        self.tables.append(table)
        self.tablenames.append(name)

    #     for table in self.tables:
    #         print(table.name)
        return table

    
    # """
    # # Deletes the specified table
    # """
    def drop_table(self, name):
        for table in self.tables: 
            if table.name == name: 
                self.tables.remove(table)
        print(self.tables)
        pass

    
    # """
    # # Returns table with the passed name
    # """
    def get_table(self, name):
        for table in self.tables: 
            if table.name == name: 
                return table 
        print(self.tables)

        self.bufferpool.get_table_dir(name)
        pass
