from lstore.index import Index
#from lstore.index import Page #added Page because pdf says Table uses Page internally
from time import time
from lstore.page import PageRange
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
        self.page_directory = {}
        self.index = Index(self)

        #array for the page ranges
        self.pageRange = []
        
        #want to keep track of current page range, current BP, current record
        self.curPageRange = 0
        self.curBP = 0
        self.curRecord = 0

        self.add_page_range(num_columns)
        pass
    

    #added numCols to arguments because creating a PageRange requires numCols argument
    def add_page_range(self, numCols):
        page_range = PageRange(numCols) 

        self.pageRange.append(page_range) #adding new page range to page range array
        self.curPageRange = len(self.pageRange) - 1 #update current page range
        
    def get_page_range(self):
        if self.pageRange[self.curPageRange].has_capacity():
            return self.pageRange[self.curPageRange]
        else:
            self.add_page_range(self.num_columns)
            return self.pageRange[self.curPageRange]
            

    def updateCurBP(self):
        self.curBP = self.pageRange[self.curPageRange].num_base_pages - 1 #update current Base Page based on current page range
        if self.curBP == -1:
            self.curBP = 0 #in case that numbasepages is 0 and becomes -1
    
    #Does this need to be a pointer???        
    def getCurBP(self):
        return self.pageRange[self.curPageRange].basePages[self.curBP]

    def updateCurRecord(self):
        self.curRecord = self.pageRange[self.curPageRange].basePages[self.curBP].num_records 

    def createBP_RID(self):
       
        tupleRID = (self.curPageRange, self.curBP, self.curRecord, 'b') 
        self.pageRange[self.curPageRange].basePages[self.curBP].rid[self.curRecord] = tupleRID
        return tupleRID
   
    def find_record(self, rid, projected_columns_index):
        #Assuming we have rid of the base page record
         # updating rid to the latest version of the record. 
        self.page_directory[rid] = rid
        record = []
        if(rid[3] == 'b'):
            for i in range(len(projected_columns_index)):
                if (projected_columns_index[i] == 1):
                    bytearray = self.pageRange[rid[0]].basePages[rid[1]].pages[i].data
                    value = int.from_bytes(bytearray[rid[2] * 8:rid[2] * 8 + 8], byteorder='big')
                    record.append(value)
        else:
           for i in range(len(projected_columns_index)):
                if (projected_columns_index[i] == 1):
                    bytearray = self.pageRange[rid[0]].tailPages[rid[1]].pages[i].data
                    value = int.from_bytes(bytearray[rid[2] * 8:rid[2] * 8 + 8], byteorder='big')
                    record.append(value) 
        return record 
        pass
    
    def get_key(self, RID):
        #return self.pageRange[RID[0]].basePages[RID[1]].pages[0] + 8*RID[3]
        return self.pageRange[RID[0]].basePages[RID[1]].pages[0].data[8*RID[2]]
    
    def insertRec(self, start_time, schema_encoding, *columns):
        if self.getCurBP().has_capacity() == False:                #checks if current BP is full
            if self.pageRange[self.curPageRange].has_capacity():  #checks if current page range is full
                 self.pageRange[self.curPageRange].add_base_page(self.num_columns) #if not, adds base page
                 self.updateCurBP()
                 self.updateCurRecord()                             #updates current BP to new BP
            else: #if is
                 self.add_page_range(self.num_columns)          #add a new page range
                 self.updateCurBP()                             #adding a new page range should have set the current page range 
                 self.updateCurRecord()
                                                                # to the new one and added a new base page to it
                                                                      
        RID  = self.createBP_RID()                        #create RID for inserted record (inserts can only be for BP)
        self.page_directory[RID] = RID
        indirection = RID                                       #add RID to indirection column since this is insert, not update
        self.getCurBP().insertRecBP(RID, start_time, schema_encoding, indirection, *columns) #now insert                                           #update table's numRecords
        self.updateCurRecord()                                  #update record index for current BP
        key = columns[0]
        self.index.add_node(key,RID)
    
    def __merge(self, PageRangeNumber):
        # function called on a page range when a certain limit is reached
        # assume that where this function is called, we already have implemented a diffferent thread for merging, and the pagerange ID to be merged is passed as an integer. 
        PageRange = self.pageRange[PageRangeNumber] # get page range from self object. This should later be changed to pulling data from the file on a disk

        # 
        print("merge is happening")

        pass
 
