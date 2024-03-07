from lstore.index import Index
from lstore.page import Page #added Page because pdf says Table uses Page internally
from time import time
from lstore.page import PageRange
from lstore.bufferpool import *
import os
import numpy as np
import array
import struct

TABLEKEY = 0
TABLENUMCOL = 1
TABLECURPG = 2
TABLECURBP = 3
TABLECURREC = 4


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
    def __init__(self, name, num_columns, key, bufferpool, isNew):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool = bufferpool
        self.num_pageRanges = 0 # can be implemented using length
        self.pageRangePaths = []
        self.pageRange = []
        self.curPageRange = 0
        self.curBP = 0
        self.curRecord = 0
        self.curFrameIndexBP = 0
        self.curFrameIndexTP = 0
        if(isNew):
            self.add_page_range(self.num_columns)

    
    def pullpagerangesfromdisk(self, path): 
        for entry in os.listdir(path):
                specifictablepath = os.path.join(path, entry)
                if os.path.isdir(specifictablepath):
                    self.pageRangePaths.append(specifictablepath)
                    self.num_pageRanges += 1   
    #added numCols to arguments because creating a PageRange requires numCols argument
    #added numCols to arguments because creating a PageRange requires numCols argument
    def add_page_range(self, numCols):
        #page_range = PageRange(numCols) 

        #self.pageRange.append(page_range) #adding new page range to page range array
        
        #self.curPageRange = len(self.pageRange) - 1 #update current page range

        self.bufferpool.allocate_page_range(self.num_columns, self.curPageRange) #when adding page range, need to allocate space in disk
        self.num_pageRanges += 1 #keep track of page range index
        
    def get_page_range(self):
        if self.pageRange[self.curPageRange].has_capacity():
            return self.pageRange[self.curPageRange]
        else:
            self.add_page_range(self.num_columns)
            return self.pageRange[self.curPageRange]
        

    def savemetadata(self, path):
        arr = []
        arr.append(self.key)
        arr.append(self.num_columns)
        arr.append(self.curPageRange)
        arr.append(self.curBP)
        arr.append(self.curRecord)
        metadata = array.array('i', arr)

        # Open the file in binary write mode
        with open(path, 'wb') as file:
            binary_data = metadata.tobytes()
            file.write(binary_data)
        pass

    def updateCurBP(self):
        #self.curBP = self.pageRange[self.curPageRange].num_base_pages - 1 #update current Base Page based on current page range
        self.curBP += 1
        if self.curBP == -1:
            self.curBP = 0 #in case that numbasepages is 0 and becomes -1
    
    #Does this need to be a pointer???        
    def getCurBP(self):
        return self.pageRange[self.curPageRange].basePages[self.curBP]

    def updateCurRecord(self):
        #self.curRecord = self.bufferpool.frames[frame_index].numRecords #should be frame[frame_index found through curBP]
        self.curRecord += 1

    def createBP_RID(self):
       
        tupleRID = (self.curPageRange, self.curBP, self.bufferpool.frames[self.curFrameIndexBP].numRecords, 'b') 
        #self.pageRange[self.curPageRange].basePages[self.curBP].rid[self.curRecord] = tupleRID
        return tupleRID
    
    def find_record(self, key,  rid, projected_columns_index, TPS):
        #Assuming we have rid of the base page record
         # updating rid to the latest version of the record. 
        rid = self.page_directory[rid]
        if(rid[3]== 't'):
            self.bufferpool.load_tail_page(rid[0], rid[1], self.num_columns)
            key_directory = (rid[0], rid[1], 't')
            if self.greaterthan(TPS, [rid[1], rid[2]]):     
                rid = self.bufferpool.extractBaseRID(key_directory, self.num_columns, rid[2])
                self.bufferpool.load_base_page(rid[0], rid[1], self.num_columns, self.name)
                key_directory = (rid[0], rid[1], 'b')
                data = self.bufferpool.extractData(key_directory, self.num_columns, rid[2])
            else:
                data = self.bufferpool.extractdata(key_directory, self.num_columns, rid[2])

        record = []
        
        if(rid[3] == 'b'):
            self.curFrameIndexBP = self.bufferpool.load_base_page(rid[0], rid[1], self.num_columns, self.name)
            key_directory = (rid[0], rid[1], 'b')
            data = self.bufferpool.extractdata(self.curFrameIndexBP, self.num_columns, rid[2])

        for i in range(len(projected_columns_index)):
            if (projected_columns_index[i] == 1):
                record.append(data[i])
        retval = Record(key, rid, record)
        return retval 
        pass

    
    def find_tail_rec_for_merge(self, rid):
        record = []
       
        for i in range(len(self.num_columns)):
                    bytearray = self.pageRange[rid[0]].tailPages[rid[1]].pages[i].data                        
                    value = int.from_bytes(bytearray[rid[2] * 8:rid[2] * 8 + 8], byteorder='big')
                    record.append(value) 
        return record

    def curBP_has_Capacity(self):
        if self.bufferpool.frames[self.curFrameIndexBP].numRecords < 512: #should be frame[frame_index found through curBP]
            return True
        else:
            return False

    def curPR_has_Capacity(self):
        if self.bufferpool.frames[15].numRecords < 512: #need a better way to check, frames[15] just checks the 15th index in the bufferpool, we need to check the 15th base page in a page range
            #could have an array of pageranges that have true or false in them (or 1 and 0 for easier storing)
            return True
        else:
            return False

    
    def get_key(self, RID):
        #return self.pageRange[RID[0]].basePages[RID[1]].pages[0] + 8*RID[3]
        return self.pageRange[RID[0]].basePages[RID[1]].pages[0].data[8*RID[2]]
    
    def insertRec(self, start_time, schema_encoding, *columns):
        # if self.getCurBP().has_capacity() == False:                #checks if current BP is full
        self.curFrameIndexBP = self.bufferpool.load_base_page(self.curPageRange, self.curBP, self.num_columns, self.name)

        # if self.curBP_has_Capacity(frame_index) == False:
        #     if self.curPR_has_Capacity(self.curPageRange):
        #         self.curBP += 1
        #         self.bufferpool.load_base_page(self.curPageRange, self.curBP, self.num_columns, self.name, self.curRecord)
        #         self.updateCurRecord()
        #     else:
        #         self.curPageRange += 1
        #         self.bufferpool.allocate_page_range(self.num_columns, self.curPageRange)
        #         self.curBP = 0
        #         self.updateCurRecord()



        RID = self.createBP_RID()
        self.page_directory[RID] = RID
        indirection = RID
        self.bufferpool.insertRecBP(RID, start_time, schema_encoding, indirection, *columns, numColumns = self.num_columns)
        print("RID " + str(self.curPageRange) + " " + str(self.curBP) + " " + str(self.bufferpool.frames[self.curFrameIndexBP].numRecords - 1))
        self.updateCurRecord() #make sure to increment self.bufferpool.frames[curBP].numRecords by 1 in insertRecBP
        if self.bufferpool.frames[self.curFrameIndexBP].has_capacity() == FALSE:
            if self.curRecord == 8192:
                self.curPageRange += 1
                self.bufferpool.allocate_page_range(self.num_columns, self.curPageRange)
                self.curRecord = 0
                self.curBP = 0
            else:
                self.updateCurBP()
        key = columns[0]
        self.index.add_node(key,RID)

        
        #     if self.pageRange[self.curPageRange].has_capacity():  #checks if current page range is full
        #          self.pageRange[self.curPageRange].add_base_page(self.num_columns) #if not, adds base page
        #          self.updateCurBP()
        #          self.updateCurRecord()                             #updates current BP to new BP
        #     else: #if is
        #          self.add_page_range(self.num_columns)          #add a new page range
        #          self.updateCurBP()                             #adding a new page range should have set the current page range 
        #          self.updateCurRecord()
        #                                                         # to the new one and added a new base page to it
                                                                      
        # RID  = self.createBP_RID()                        #create RID for inserted record (inserts can only be for BP)
        # self.page_directory[RID] = RID
        # indirection = RID                                       #add RID to indirection column since this is insert, not update
        # self.getCurBP().insertRecBP(RID, start_time, schema_encoding, indirection, *columns) #now insert                                           #update table's numRecords
        # self.updateCurRecord()                                  #update record index for current BP
        # key = columns[0]
        # self.index.add_node(key,RID)

    def updateRec(self, rid, baseRID, primary_key, *columns):
        projected_columns_index = [] #creates array to tell which columns need to be raplced with base page record entry (done later)
        for i in range(self.num_columns):
            projected_columns_index.append(1)
        
        self.curFrameIndexBP = self.bufferpool.load_base_page(rid[0], rid[1], self.num_columns, self.name) #load base page of record to update and return record for the base page rid (also sets self.curFrameIndex to bp we loaded)
        currentRID = self.bufferpool.frames[self.curFrameIndexBP].indirection[rid[2]] #find the rid in the indirection column of the record we're updating to get the most recently updated rid
        try:    
            record = self.find_record(primary_key, currentRID, projected_columns_index, self.bufferpool.frames[self.curFrameIndexBP].TPS[rid[2]]) #finds record using indirection in Base Record
        except:
            tempTPS = (currentRID[0], currentRID[1] + 1)
            record = self.find_record(primary_key, currentRID, projected_columns_index, tempTPS) #finds record using indirection in Base Record

        numTPS = 0 #count how many tail pages are in the directory
        for path in os.scandir(f"{self.bufferpool.current_table_path}/pageRange{rid[0]}/tailPages"):
            if path.is_file():
                numTPS += 1

        if not numTPS == 0:
            numTPS -= 1 #make sure we're checking the existing tail page first because it might have capacity

        self.curFrameIndexTP = self.bufferpool.load_tail_page(rid[0], numTPS, self.num_columns, self.name) #load tail page we need, will also check if current tail page is full, and if is, will allocate space for a new one and add to there instead

        if not self.bufferpool.frames[self.curFrameIndexTP].has_capacity(): #if tail page is full, allocate new one, and load it
            numTPS += 1
            self.bufferpool.allocate_tail_page(self.num_columns, rid[0], numTPS)
            self.curFrameIndexTP = self.bufferpool.load_tail_page(rid[0], numTPS, self.num_columns, self.name)

        updateRID = (rid[0], numTPS, self.bufferpool.frames[self.curFrameIndexTP], 't')
        self.bufferpool.insertRecTP(record, rid, updateRID, currentRID, baseRID, self.curFrameIndexBP, *columns)

        self.page_directory[updateRID] = updateRID
        # self.table.bufferpool.frames[self.curFrameIndexTP].indirection.append(currentRID) #make current indirection of bp the indirection of tp (DOING IT IN BUFFERPOOL.PY)
        # self.table.bufferpool.frames[self.curFrameIndexTP].BaseRID.append(baseRID)


        

        


        



    
    def __merge(self, PageRangeIndex):
        # function called on a page range when a certain limit is reached
        # assume that where this function is called, we already have implemented a diffferent thread for merging, and the pagerange ID to be merged is passed as an integer. 
        PageRange = self.pageRange[PageRangeIndex] # get page range from self object. This should later be changed to pulling data from the file on a disk
        newpagedirectory = self.table.page_directory
        current_tail_page = len(PageRange.tailPages) - 1
        current_tail_record = len(PageRange.tailPages[current_tail_page].rid) - 1
        oldTPS = PageRange.TPS 
        newTPS = [current_tail_page, current_tail_record]

    def write_record_to_disk(self): #will add attributes as we go
        pass

    def greaterthan(self, a, b):
        if(a[0] > b[0]):
            return True
        elif(a[0] == b[0]):
            if(a[1] > b[1]):
                 return True
        return False
            
        newBasePages = PageRange.basePages #if not copy metadata then what do
        bitSignal =  np.array(0, 8192)
        while(current_tail_record >= 0 and current_tail_page >= 0 and greaterthan([current_tail_page, current_tail_record ] , oldTPS) ):
            baseRID = self.pageRange[PageRangeIndex].tailPages[current_tail_page].baseRID[current_tail_record]; # implement baseRID everywhere
            baseRID = self.page_directory[baseRID]
            newPhysicalLocation = [baseRID[0], baseRID[1] + 16, baseRID[2], baseRID[3]] # loop through page directory later to implement this
            if(bitSignal[(baseRID[1] % 16) * 512 + (baseRID[2])] == 0):
                bitSignal[(baseRID[1] % 16) * 512 + (baseRID[2])] = 1
                newpagedirectory[baseRID] = newPhysicalLocation
                updatedvalues = self.find_tail_rec_for_merge([PageRangeIndex, current_tail_page,current_tail_record, 't' ])
                for i in range(len(self.num_columns)):
                    newBasePages[baseRID[1] % 16].pages[i][baseRID[1]: baseRID[1] + 8] = updatedvalues[i].to_bytes(8, byteorder='big')
                newBasePages[baseRID[1] % 16].indirection[baseRID[1]] = [PageRangeIndex, current_tail_page, current_tail_record, 't']
            current_tail_record -= 1 
            if current_tail_record == -1: 
                current_tail_record = 511
                current_tail_record =-1
        self.pageRange.TPS = newTPS; 
        for newBasePage in newBasePages: 
            self.pageRange[PageRangeIndex].basePages.append(newBasePage)


        # NEXT STEP IS TO UPDATE PAGE DIRECTORY. (do this by swapping newpagedirectory and self.table.page_directory on the main thread)INDIRECTION COLUMNS FOR THESE UPDATES MADE ABOVE ARE flimsy, and sum, select, update need to be changed to check TPS as well
              



            
            

            
            


        #  # steps to implement: 
        #     figure out the background thread part
        #     create a duplicate page range with merged pages. 
        #     implement TPS for consolidated merged pages. 
        #     update indirection columns to actually be correct. 
        #     update the page directory on the main thread. 
        #     add page range, update current pages?
        print("merge is happening")

        pass
