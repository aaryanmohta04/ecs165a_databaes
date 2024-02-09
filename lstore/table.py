from lstore.index import Index
from lstore.index import Page #added Page because pdf says Table uses Page internally
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0

        page_range = PageRange(num_columns) 
        pass
    
    # If page_directory holds IDs of page ranges...
    # Possible implementation of page_directory -> RID
    # Not sure how to assign an ID to tail pages or if that is even needed
    # Would need to make sure tail page ID can't be the same as any 
    # already assigned or future IDs
    # EX)  Page Range1: 0-64000 but holds id = 0, Page Range 2 holds ID 64001

    #added numCols to arguments because creating a PageRange requires numCols argument
    def add_page_range(self, numCols):
        page_range = PageRange(numCols) 
        #page_range.id = #can make it equal to the value of the last index in page_directory + 1
        self.page_directory.append(page_range.id)
        num_records += MAX_RECORDS_PER_PAGE_RANGE
        
    def get_page_range(self, rid)
        # Maybe a tree or hash table for the page_directory?
        #
        return page_range
        
    def find_record(self, rid):
        # This seems inefficient so maybe there is a better way
        # If this works, need to add these 3 methods:
        page_range = get_page_range(rid)
        page = get_page(page_range, rid)
        record = get_record(page)
        

    def __merge(self):
        print("merge is happening")
        pass
 
