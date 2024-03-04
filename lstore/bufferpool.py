from asyncio.windows_events import NULL
from pickle import FALSE, TRUE
# from lstore.table import Table
from lstore.page import Page
from datetime import datetime
import os

# How many?
FRAMECOUNT = 100
#need to make table directory with all tables and relevent table values with pickle

class Bufferpool:
    
    def __init__(self, path_to_root):
        
        self.frames = []
        self.numFrames = 0
        self.path_to_root = path_to_root
        self.current_table_path = None #to keep track of current table, we're working in
        self.current_total_path = None
        self.frame_directory = [] #put 
        self.frame_info = [None] * 100

    def has_capacity(self):
        if self.numFrames < FRAMECOUNT:
            return True
        return False
    
    def start_db_dir(self):
        if os.path.isdir(self.path_to_root) == FALSE: #if path doesn't exist
            os.mkdir(self.path_to_root) #create it (./ECS165) (will we need parent path i.e. /home/user etc?)
        pass

    def start_table_dir(self, name, numCols): #makes directory for table within db directory
        path = self.path_to_root + f"/tables/"
        path = os.path.join(path, name)
        if not os.path.exists(path):
            os.mkdir(path)
        self.current_table_path = path

    def allocate_page_range(self, numCols, pageRangeIndex): #allocates enough space for 16 BPs in new page range
        page_range_path = f"{self.current_table_path}/pageRange{pageRangeIndex}"
        os.mkdir(page_range_path)

        for i in range(16):
            bp_file_location = f"{page_range_path}/basePage{i}.bin"
            bp_file = open(bp_file_location, "wb")

            for j in range(numCols + 11): #number of columns + rid(4), indirection, time, schema (need base rid column too?), metadata: TPS, numRecords (one page with metadata single elements)
                bp_file.write(bytearray(4096))

            bp_file.close()

        tail_page_path = f"{page_range_path}/tailPages"
        os.mkdir(tail_page_path)

    def allocate_tail_page(self, numCols, pageRangeIndex, numTPs):

        tp_file_location = f"{self.current_table_path}/pageRange{pageRangeIndex}/tailPages/tailPage{numTPs}.bin"
        tp_file = open(tp_file_location, "wb")

        for i in range(numCols + 14): #number of cols + rid(4), indirection, schema, base rid, metadata: numRecords
            tp_file.write(bytearray(4096))

        tp_file.close()
        

    def get_table_dir(self, name): #sets the current path to look into current table directory
        path = os.path.join(self.path_to_root, name)
        if os.path.isdir(path):
            self.current_table_path = path

    def extractRID(self, key_directory, num_columns, recordnumber): 
        newrid = []
        frame_index = self.get_frame_index(key_directory)
        for i in range(3):
            #frame_index = self.frame_directory[key_directory]
            x = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + i][recordnumber*8:(recordnumber + 1)*8], 'big')
            newrid.append(x)
        return newrid
    
    def extractIndirection(self, key_directory, num_columns, recordnumber): 
        newrid = []
        frame_index = self.get_frame_index(key_directory)
        for i in range(3):
            #frame_index = self.frame_directory[key_directory]
            x = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + i + 4][recordnumber*8:(recordnumber + 1)*8], 'big')
            newrid.append(x)
        return newrid
    
    def extractBaseRID(self,key_directory, num_columns, recordnumber): 
        newrid = []
        frame_index = self.get_frame_index(key_directory)
        for i in range(3):
            #frame_index = self.frame_directory[key_directory]
            x = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + i + 8][recordnumber*8:(recordnumber + 1)*8], 'big')
            newrid.append(x)
        return newrid

    def extractTPS(self, key_directory, num_columns):
        #frame_index = self.frame_directory[key_directory]
        frame_index = self.get_frame_index(key_directory)
        if(len(((self.frames[frame_index]).frameData)) >= num_columns + 11):
            x = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + 10][0:8], 'big')
            y = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + 10][8:16], 'big')
        else: 
            x = y = 0
        return [x,y]
    
    def extractRecordCount(self, key_directory, num_columns):
        x = 0
        frame_index = self.get_frame_index(key_directory)
        if key_directory[2] == 'b':
            if(len(((self.frames[frame_index]).frameData)) >= num_columns + 11):
                x = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + 10][16:24], 'big')
        elif key_directory[2] == 't':
            if(len(((self.frames[frame_index]).frameData)) >= num_columns + 14):
                x = int.from_bytes(((self.frames[frame_index]).frameData)[num_columns + 13][0:8], 'big')
        
        return x
    
#use frame directory
    def in_pool(self, key):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key:
                return True
        return False
            
    def get_frame_index(self, key_directory):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key_directory:
                return i
       
    
    #LRU, send to disk
    def evict_page(self):
        page_to_evict = self.frames[0]
        evict_index = 0
        for i in range(len(self.frames) - 1):
            if self.frames[i] > self.frames[i + 1]:
                if self.frames[i + 1].isPinned() == FALSE:
                    page_to_evict = self.frames[i + 1]
                    evict_index = i + 1
        if(page_to_evict.dirtyBit == TRUE):
            #self.frames[evict_index].write_to_disk(self.current_total_path, self.frames[evict_index].frameData) #writes data to disk
            self.frames[evict_index].pin_page()
            if self.frame_info[evict_index][2] == 'b':
                    path = f"{self.path_to_root}/pageRange{self.frame_info[evict_index][0]}/basePage{self.frame_info[evict_index][1]}.bin"
                    for j in range(len(self.frames[evict_index].frameData)):
                        self.frames[evict_index].frameData[j].write_to_disk(path, self.frames[evict_index].frameData[j].data, j)
                    self.write_rid(path, len(self.frames[evict_index].frameData), evict_index)
                    self.write_indirection(path, len(self.frames[evict_index].frameData) + 4, evict_index)
                    self.write_start_time(path, len(self.frames[evict_index].frameData) + 8, evict_index)
                    self.write_schema_encoding(path. len(self.frames[evict_index].frameData) + 9, evict_index)
                    self.write_TPS(path, len(self.frames[evict_index].frameData) + 10, i)
                    self.write_numRecords(path, len(self.frames[evict_index].frameData) + 10, evict_index, 2)

            elif self.frame_info[evict_index][2] == 't':
                    path = f"{self.path_to_root}/pageRange{self.frame_info[evict_index][0]}/tailPages/tailPage{self.frame_info[evict_index][1]}.bin"
                    for j in range(len(self.frames[evict_index].frameData)):
                        self.frames[evict_index].frameData[j].write_to_disk(path, self.frames[evict_index].frameData[j].data, j)
                    self.write_rid(path, len(self.frames[evict_index].frameData), evict_index)
                    self.write_indirection(path, len(self.frames[evict_index].frameData) + 4, evict_index)
                    self.write_baseRid(path, len(self.frames[evict_index].frameData) + 8, evict_index)
                    self.write_schema_encoding(path, len(self.frames[evict_index].frameData) + 12, evict_index)
                    self.write_numRecords(path, len(self.frames[evict_index].frameData) + 13, evict_index, 0)
            self.frames[evict_index].unpin_page()
        return evict_index
        #I'm thinking that if it's not dirty, we can just write over the info when we load, so we can just leave it and return which index it's at
    
    #Bring page in from disk, if full, evict a page first
    def load_base_page(self, page_range_index, base_page_index, numColumns, table_name, numRecords):
        path_to_page = self.current_table_path + f"/pageRange{page_range_index}/basePage{base_page_index}.bin"
        self.current_total_path = path_to_page
        d_key = (page_range_index, base_page_index, 'b')
        if not self.in_pool(d_key):
            if not self.has_capacity():
                frame_index = self.evict_page()
                #d_key_remove = self.frame_info[frame_index] #to remove the old info from frame_directory (frame_info will just be overwritten)
                #self.frame_directory.remove(d_key_remove)
                self.frames[frame_index] = Frame(path_to_page, numColumns)
            else:
                frame_index = self.numFrames
                self.frames.append(Frame(path_to_page, numColumns))
        else: 
            frame_index = self.get_frame_index(d_key)

        self.frames[frame_index].pin_page()
        for i in range(numColumns):
            self.frames[frame_index].frameData[i] = Page()
            self.frames[frame_index].frameData[i].read_from_disk(path_to_page, i) #read data from page into frame
            
        directory_key = (page_range_index, base_page_index, 'b')
        self.frame_info[frame_index] = directory_key
        if not numRecords == 0:
            self.frame_info[frame_index] = directory_key
            self.frames[frame_index].TPS = self.extractTPS(directory_key, numColumns)
            self.frames[frame_index].numRecords = self.extractRecordCount(directory_key, numColumns)

            for i in range(self.frames[frame_index].numRecords):
                self.frames[frame_index].rid[i] = self.extractRID(directory_key, numColumns, 0)
                self.frames[frame_index].indirection[i] = self.extractIndirection(directory_key, numColumns, 0)
        self.frames[frame_index].unpin_page()

        pass

    def load_tail_page(self, page_range_index, tail_page_index, numColumns, table_name):
        path_to_page = path_to_page = self.current_table_path + f"/pageRange{page_range_index}/tailPages/tailPage{tail_page_index}.bin"
        self.current_total_path = path_to_page
        d_key = (page_range_index, tail_page_index, 't')
        if not self.in_pool(d_key):
            if not self.has_capacity():
                frame_index = self.evict_page()
                #d_key_remove = self.frame_info[frame_index]
                #self.frame_directory.remove(d_key_remove)
                self.frames[frame_index] = Frame(path_to_page, numColumns)
            else:
                frame_index = self.numFrames
                self.frames.append(Frame(path_to_page, numColumns))
        

            self.frames[frame_index].pin_page()
            
        for i in range(numColumns):
            self.frames[frame_index].frameData[i] = Page()
            self.frames[frame_index].frameData[i].read_from_disk(path_to_page, i) #read data from page into frame
            
        directory_key = (page_range_index, tail_page_index, 't')
        #self.frame_directory[directory_key] = frame_index

        self.frame_info[frame_index] = directory_key

        self.frames[frame_index].numRecords = self.extractRecordCount(directory_key, numColumns)

        for i in range(self.frames[frame_index.numRecords]):
            self.frames[frame_index].rid[i] = self.extractRID(directory_key, numColumns, 0)
            self.frames[frame_index].indirection[i] = self.extractIndirection(directory_key, numColumns, 0)
            self.frames[frame_index].BaseRID[i] = self.extractIndirection(directory_key, numColumns, 0)
        
        self.frames[frame_index].unpin_page()
    

    def insertRecBP(self, RID, start_time, schema_encoding, indirection, *columns, numColumns):
        key_directory = (RID[0], RID[1], 'b')
        print(str(key_directory))
        frame_index = self.get_frame_index(key_directory=key_directory)
        self.frames[frame_index].pin_page()
        for i in range(numColumns): #iterates through number of columns and writes data in *columns to corresponding page in page[] 
            self.frames[frame_index].frameData[i].write(columns[i])
            print("writing" + str(columns[i]))
        self.frames[frame_index].numRecords += 1
        self.frames[frame_index].rid.append(RID)
        self.frames[frame_index].start_time.append(start_time)
        self.frames[frame_index].schema_encoding.append(schema_encoding)
        self.frames[frame_index].indirection.append(indirection)
        print(f"this is the frame index: {frame_index} and the rid list is : {str(self.frames[frame_index].rid)} +{str(self.frames[frame_index].start_time)}")
        print(f"this is the data of the first column{str(self.frames[frame_index].frameData[0].data)}")
        self.frames[frame_index].set_dirty_bit()
        self.frames[frame_index].unpin_page()


    
    # def write_to_disk(self, frame_index):
    #     frame = self.frames[frame_index]
    #     columns = frame.columns
    #     path_to_page = frame.path
    #     if frame.dirtyBit == True:
    #         bin = open(path_to_page, "wb")
    #         for i in range(len(columns)):
    #             # Write base/tail page
    #             pass
    #         bin.close()
    
    #Write all pages to disk
    def write_rid(self, path, numCols, curIndex): #curIndex refers to current BP index
        for i in range(4):
            tempPage = Page()
            for j in range(self.frames[curIndex].numRecords):
                tempPage.write(self.frames.rid[j][i])
            
            tempPage.write_to_disk(path, tempPage.data, numCols + i)
            
    def write_indirection(self, path, numCols, curIndex):
        for i in range(4):
            tempPage = Page()
            for j in range(self.frames[curIndex].numRecords):
                tempPage.write(self.frames.indirection[j][i])
            
            tempPage.write_to_disk(path, tempPage.data, numCols + i)

    def write_baseRid(self, path, numCols, curIndex):
        for i in range(4):
            tempPage = Page()
            for j in range(self.frames[curIndex].numRecords):
                tempPage.write(self.frames.BaseRID[j][i])
            
            tempPage.write_to_disk(path, tempPage.data, numCols + i)

    def write_start_time(self, path, numCols, curIndex):
        tempPage = Page()
        for j in range(self.frames.curIndex.numRecords):
            tempPage.write(self.frames.start_time[j])

        tempPage.write_to_disk(path, tempPage.data, numCols)

    def write_schema_encoding(self, path, numCols, curIndex):
        tempPage = Page()
        for j in range(self.frames.curIndex.numRecords):
            tempPage.write(int)(self.frames.schema_encoding[j])

        tempPage.write_to_disk(path, tempPage.data, numCols)

    def write_TPS(self, path, numCols, curIndex):
        for i in range(2):
            tempPage = Page()
            tempPage.write(self.frames.TPS[i])
            tempPage.write_to_disk_record(path, tempPage.data, numCols, i)

    def write_numRecords(self, path, numCols, curIndex, row):
        tempPage = Page()
        tempPage.write(self.frames.numRecords)
        tempPage.write_to_disk_record(path, tempPage.data, numCols, row)

    def close(self):
        for i in range(len(self.frames)):
            if self.frames[i].dirtyBit == True:
                if self.frame_info[i][2] == 'b':
                    path = f"{self.path_to_root}/pageRange{self.frame_info[i][0]}/basePage{self.frame_info[i][1]}.bin"
                    for j in range(len(self.frames[i].frameData)):
                        self.frames[i].frameData[j].write_to_disk(path, self.frames[i].frameData[j].data, j)
                    self.write_rid(path, len(self.frames[i].frameData), i)
                    self.write_indirection(path, len(self.frames[i].frameData) + 4, i)
                    self.write_start_time(path, len(self.frames[i].frameData) + 8, i)
                    self.write_schema_encoding(path. len(self.frames[i].frameData) + 9, i)
                    self.write_TPS(path, len(self.frames[i].frameData) + 10, i)
                    self.write_numRecords(path, len(self.frames[i].frameData) + 10, i, 2)

                elif self.frame_info[i][2] == 't':
                    path = f"{self.path_to_root}/pageRange{self.frame_info[i][0]}/tailPages/tailPage{self.frame_info[i][1]}.bin"
                    for j in range(len(self.frames[i].frameData)):
                        self.frames[i].frameData[j].write_to_disk(path, self.frames[i].frameData[j].data, j)
                    self.write_rid(path, len(self.frames[i].frameData), i)
                    self.write_indirection(path, len(self.frames[i].frameData) + 4, i)
                    self.write_baseRid(path, len(self.frames[i].frameData) + 8, i)
                    self.write_schema_encoding(path, len(self.frames[i].frameData) + 12, i)
                    self.write_numRecords(path, len(self.frames[i].frameData) + 13, i, 0)
        pass

class Frame:

    def __init__(self, path, numColumns):
        self.frameData = [None] * numColumns #like pages[]
        self.TPS = 0
        self.numRecords = 0
        self.rid = []
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.BaseRID = []
        self.dirtyBit = False
        self.pinNum = 0
        self.path = path
        self.columns = [None] * numColumns
        self.lastAccess = 0
        self.numColumns = numColumns
        
    def set_dirty_bit(self):
        if self.dirtyBit == True:
            self.dirtyBit = False
        else:
            self.dirtyBit = True
        
    def pin_page(self):
        self.pinNum += 1
        self.lastAccess = datetime.now()

    def unpin_page(self):
        self.pinNum -= 1

    def isPinned(self):
        if self.pinNum == 0:
            return False
        else:
            return True
