from lstore.page import *


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
        return false
    
    
    #Select LRU(or whatever method), send to disk
    def evict_page(self):
        pass
    
    #Bring page in from disk, if full, evict a page first
    def load_page(self, page_range_index, base_page_index, numColumns):
        path_to_page = f"{self.path_to_root}/page_range_{page_range_index}/" \
                           f"base_page_{base_page_index}.bin"
        if self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(path,numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(path, numColumns))
        
<<<<<<< Updated upstream
        self.frames[frame_index].pin_page()
        self.frames[frame_index].pin_page()
=======
        self.frames[frame_index].pin_page()

        for i in numColumns:
            #Read in values
        
        directory_key = (page_range_index, base_page_index)
        self.frame_directory[directory_key] = frame_index
        
        self.frames[frame_index].unpin_page()
>>>>>>> Stashed changes
        
        pass
    
    def write_to_disk(self, frame_index):
        frame = self.frames[frame_index]
        columns = frame.columns
        path_to_page = frame.path
        if frame.dirtyBit == True:
            bin = open(path_to_page, "wb")
            for i in range(len(columns)):
                # Write base/tail page
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
