from lstore.index import Index
from lstore.index import Page #added Page because pdf says Table uses Page internally
from time import time

import numpy as np

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
        self.page_directory = np.array([])
        self.index = Index(self)
        self.num_records = 0

        add_page_range(self, num_columns)
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
        page_range.id = len(self.page_directory) * 64000 #1. 0 2. 64000 3. 128000 etc
        self.page_directory = np.append(self.page_directory([page_range.id]))
        num_records += MAX_RECORDS_PER_PAGE_RANGE
        
    def get_page_range(self, rid):
        # Maybe a tree or hash table for the page_directory?
        #could implement a faster search alrgorithm than just going through page_directory one by one
        for i in len(self.page_directory) - 1:
            if rid > self.page_directory[i] and rid < self.page_directory[i + 1]:
                correct_page_range_id = self.page_directory[i] #can get the id of the page_range containing the rid but how do i acually return the right page range (where are page_ranges stored and how do i access ones already made?)
        return correct_page_range_id

    def get_page(self, page_range_id, rid):
        #idk how to access pages or records from here
        pass
        
    
    def find_record(self, rid):
        # This seems inefficient so maybe there is a better way
        # If this works, need to add these 3 methods:
        page_range_id = get_page_range(rid)
        page = get_page(page_range_id, rid)
        record = get_record(page, rid)
        pass
        

    def __merge(self):
        print("merge is happening")
        pass
 
