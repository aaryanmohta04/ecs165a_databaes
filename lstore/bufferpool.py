from asyncio.windows_events import NULL
from pickle import TRUE
from lstore.table import Table
from datetime import datetime

# How many?
FRAMECOUNT = 0

class Bufferpool:
    
    def __init__(self, path_to_root):
        
        self.frames = []
        self.numFrames = 0
        self.path_to_root = path_to_root
        self.frame_directory = {}

    def has_capacity(self):
        if self.numFrames == FRAMECOUNT:
            return True
        return False
    
    #LRU, send to disk
    def evict_page(self):
        page_to_evict = self.frames[0]
        evict_index = 0
        for i in (len(self.frames) - 1):
            if self.frames[i] > self.frames[i + 1]:
                 if self.frames[i + 1].isPinned() == FALSE:
                    page_to_evict = self.frames[i + 1]
                    evict_index = i + 1

        if(page_to_evict.dirtyBit == TRUE):
            #write contents of page to disk
            pass
        return evict_index
        #I'm thinking that if it's not dirty, we can just write over the info when we load, so we can just leave it and return which index it's at
    
    #Bring page in from disk, if full, evict a page first
    def load_page(self, page_range_index, base_page_index, numColumns, table_name):
        path_to_page = f"{self.path_to_root}/page_range_{page_range_index}/" \
                           f"base_page_{base_page_index}.bin"
        if self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(path,numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(path, numColumns))
        
        self.frames[frame_index].pin_page()
        bin = open(path, "rb")
        for i in numColumns:
            bin.seek(i * 4096)
            self.frames[frame_index].columns[i] = bytearray(bin.read(4096))
        bin.close()
        
        directory_key = (page_range_index, base_page_index)
        self.frame_directory[directory_key] = frame_index
        self.frames[frame_index].unpin_page()
    
    
    def write_to_disk(self, frame_index, numColumns):
        frame = self.frames[frame_index]
        columns = frame.columns
        path_to_page = frame.path
        if frame.dirtyBit == True:
            bin = open(path_to_page, "wb")
            for i in numColumns:
                bin.write(columns[i])
            bin.close()
    
    #Write all pages to disk
    def close(self):
        for i in range(len(self.frames)):
            if self.frames[i].dirtyBit == True:
                self.write_to_disk(i)
        pass
        
class Frame:

    def __init__(self, path, numColumns):
        
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
