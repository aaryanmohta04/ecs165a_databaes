from asyncio.windows_events import NULL
from pickle import FALSE, TRUE
# from lstore.table import Table
from lstore.page import Page
from datetime import datetime
import os

# How many?
FRAMECOUNT = 0
#need to make table directory with all tables and relevent table values with pickle

class Bufferpool:
    
    def __init__(self, path_to_root):
        
        self.frames = []
        self.numFrames = 0
        self.path_to_root = path_to_root
        self.current_table_path = None #to keep track of current table, we're working in
        self.current_total_path = None
        self.frame_directory = [] #put 
        self.frame_info = []

    def has_capacity(self):
        if self.numFrames == FRAMECOUNT:
            return True
        return False
    
    def start_db_dir(self):
        if os.path.isdir(self.path_to_root) == FALSE: #if path doesn't exist
            os.mkdir(self.path_to_root) #create it (./ECS165) (will we need parent path i.e. /home/user etc?)
        pass

    def start_table_dir(self, name, numCols): #makes directory for table within db directory
        path = os.path.join(self.path_to_root, name)
        os.mkdir(path)
        self.current_table_path = path

    def allocate_page_range(self, numCols, pageRangeIndex): #allocates enough space for 16 BPs in new page range
        page_range_path = f"{self.current_table_path}/pageRange{pageRangeIndex}"
        os.mkdir(page_range_path)

        for i in range(16):
            bp_file_location = f"{page_range_path}/basePage{i}.bin"
            bp_file = open(bp_file_location, "wb")

            for j in range(numCols + 4): #number of columns + rid, indirection, time, schema (need base rid column too?)
                bp_file.write(bytearray(4096))

            bp_file.close()

        tail_page_path = f"{page_range_path}/tailPages"
        os.mkdir(tail_page_path)

    def allocate_tail_page(self, numCols, pageRangeIndex, numTPs):

        tp_file_location = f"{self.current_table_path}/pageRange{pageRangeIndex}/tailPages/tailPage{numTPs}.bin"
        tp_file = open(tp_file_location, "wb")

        for i in range(numCols + 4): #number of cols + rid, indirection, schema, base rid
            tp_file.write(bytearray(4096))

        tp_file.close()
        

    def get_table_dir(self, name): #sets the current path to look into current table directory
        path = os.path.join(self.path_to_root, name)
        if os.path.isdir(path):
            self.current_table_path = path

    
        
#use frame directory
    def in_pool(self, rid):
        for i in len(self.frames):
            pass
    pass
            

    
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
            self.frames[evict_index].write_to_disk(self.current_total_path, self.frames[evict_index].frameData) #writes data to disk
        return evict_index
        #I'm thinking that if it's not dirty, we can just write over the info when we load, so we can just leave it and return which index it's at
    
    #Bring page in from disk, if full, evict a page first
    def load_page(self, page_range_index, base_page_index, numColumns, table_name):
        path_to_page = f"{self.path_to_root}/pageRange{page_range_index}/" \
                           f"basePage{base_page_index}.bin"
        self.current_total_path = path_to_page
        if self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(path_to_page, numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(path_to_page, numColumns))
        
        self.frames[frame_index].pin_page()
            
        for i in range(numColumns + 4):
            self.frames[frame_index].frameData[i].read_from_disk(path_to_page, i) #read data from page into frame
            
        directory_key = (page_range_index, base_page_index, 'b')
        self.frame_directory[directory_key] = frame_index

        self.frame_info[frame_index] = directory_key
        
        self.frames[frame_index].unpin_page()

        pass
    
    
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
    def close(self):
        for i in range(len(self.frames)):
            if self.frames[i].dirtyBit == True:
                if self.frame_info[i][2] == 'b':
                    path = f"{self.path_to_root}/pageRange{self.frame_directory[i][0]}/basePage{self.frame_directory[i][1]}.bin"
                    self.frames[i].write_to_disk(path, self.frames[i].frameData)
                elif self.frame_info[i][2] == 't':
                    path = f"{self.path_to_root}/pageRange{self.frame_directory[i][0]}/tailPage{self.frame_directory[i][1]}.bin"
                    self.frames[i].write_to_disk(path, self.frames[i].frameData)
        pass
        
class Frame:

    def __init__(self, path, numColumns):
        self.frameData = []
        self.dirtyBit = False
        self.pinNum = 0
        self.path = path
        self.columns = [None] * numColumns
        self.lastAccess = 0
        
    def set_dirty_bit(self):
        if dirtyBit == True:
            dirtyBit = False
        else:
            dirtyBit = True
        
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
