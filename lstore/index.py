"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import pickle
from BTrees._OOBTree import OOBTree
class Index:

    def __init__(self, table):
        # One index for each column. All are empty initially.
        self.indices = [None] *  table.num_columns
        pass
    def load_index(self, picklepath): 

        with open(picklepath, 'rb') as file:
            self.indices = pickle.load(file)

    
    def get_index(self, column_number):
        return self.indices[column_number]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        tree = self.indices[column]
        return tree.get(value, None)
    
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        tree = self.indices[0]
        RID_arr = []
        for RID in list(tree.values(min=begin, max=end)):
            RID_arr.append(RID)
        return RID_arr
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] == None:
            self.indices[column_number] = OOBTree()
        else:
            print("Column index already created")
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass


    def add_node(self, key, rid):
        if self.indices[0] == None:                      # If column has no index
            self.create_index(0)
        self.indices[0][key] = rid
        
    def update_index(self, rid, *columns):
        for key in columns:
            self.add_node(key, rid)

    def close_and_save(self,picklepath): 
         with open(picklepath, 'wb') as file:
            pickle.dump(self.indices, file) 
            