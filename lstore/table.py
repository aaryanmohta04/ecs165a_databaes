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
        self.page_directory = []
        self.index = Index(self)
        self.num_records = 0

        #array for the page ranges
        self.pageRange = []
        
        #want to keep track of current page range, current BP, current record
        self.curPageRange = 0
        self.curBP = 0
        self.curRecord = 0

        add_page_range(num_columns)
        pass
    

    #added numCols to arguments because creating a PageRange requires numCols argument
    def add_page_range(self, numCols):
        page_range = PageRange(numCols) 

        self.pageRange.append(page_range) #adding new page range to page range array
        self.curPageRange = len(self.pageRange) - 1
        self.num_records += MAX_RECORDS_PER_PAGE_RANGE
        
    def get_page_range(self):
        if self.pageRange[self.curPageRange].has_capacity():
            return self.pageRange[self.curPageRange]
        else:
            add_page_range(self.num_columns)
            return self.pageRange[self.curPageRange]
            

    def updateCurBP(self):
        self.curBP = self.pageRange[self.curPageRange].num_base_pages - 1 #update current Base Page based on current page range
        if self.curBP == -1:
            self.curBP = 0 #in case that numbasepages is 0 and becomes -1
    
    def insertRec(self, *columns): #do i need the pointer?
        if self.pageRange[self.curPageRange].hasCapacity: #checks if current page range is full
            if self.pageRange[self.curPageRange].basePages[self.curBP].hasCapacity: #checks if current BP is full
                self.pageRange[self.curPageRange].basePages[self.curBP].insertRecBP(*columns)
                
    def find_record(self, rid):
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
