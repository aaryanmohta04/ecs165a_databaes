from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
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
        table = Table(name, num_columns, key_index)
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
        pass
