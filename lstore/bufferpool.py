from lstore.page import *


# How many?
FRAMECOUNT = 0

class Bufferpool:
    
    def __init__(self, path_to_root):
        
        self.frames = []
        self.numFrames = 0
        self.path_to_root = path_to_root
        
    def has_capacity(self):
        if self.numFrames == FRAMECOUNT:
            return True
        return false
    
    #Select LRU(or whatever method), send to disk
    def evict_page(self):
        pass
    
    #Bring page in from disk, if full, evict a page first
    def load_page(self, page_range_index, base_page_index, numColumns):
        path_to_page = f"{self.path_to_root}/{table_name}/page_range_{page_range_index}/" \
                           f"base_page_{base_page_index}.bin"
        if self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(path,numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(path, numColumns))
        
        self.frames[frame_index].pin_page()
        self.frames[frame_index].pin_page()
        
        #Read in values
        for i in numColumns:
            
        pass
    
    
    #Write all pages to disk
    def close(self):
        pass
    
class Frame:

    def __init__(self, path, numColumns):
        
        self.dirtyBit = False
        self.pin = False
        self.path = path
        self.columns = [None] * numColumns
        
    def set_dirty_bit(self):
        if dirtyBit == True:
            dirtyBit = False
        else:
            dirtyBit = True
        
   def pin_page(self):
        self.pinNum += 1

    def unpin_page(self):
        self.pinNum -= 1

    def isPinned(self):
        if self.pinNum == 0:
            return False
        else:
            return True
