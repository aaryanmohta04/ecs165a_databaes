from lstore.table import Table
from lstore.bufferpool import *

class Database():

    def __init__(self):
        self.tables = []
        #self.tableDirectory = []
        self.bufferpool = None
        pass

    # Not required for milestone1
    def open(self, path):
        self.bufferpool = Bufferpool(path) #create a bufferpool object for this db
        self.bufferpool.start_dir() #initiate directory given the path (or check if it exists)
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        self.bufferpool.start_table_dir(name, num_columns) #makes table directory
        self.bufferpool.allocate_page_range(self, num_columns, 0)
        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables.append(table)

        for table in self.tables:
            print(table.name)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables: 
            if table.name == name: 
                self.tables.remove(table)
        print(self.tables)
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables: 
            if table.name == name: 
                return table 
        print(self.tables)

        self.bufferpool.get_table_dir(name)
        pass
