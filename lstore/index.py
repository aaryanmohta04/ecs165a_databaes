"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass
    
    def get_index(self, column_number):
        return self.indices[column_number]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        tree = self.indices[column]
        return tree[value]
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        tree = self.indices[column]
        RID_arr = []
        for RID in list(tree.values(min=begin, max=end)):
            RID_arr += RID
        return RID_arr
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] == None:
            self.indices[column_number] == OOBTree()
        else:
            print("Column index already created")
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass
