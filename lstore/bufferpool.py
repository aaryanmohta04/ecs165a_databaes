from lstore.page import *


# How many?
FRAMECOUNT = 0

class Bufferpool:
    
    def __init__(self):
        
        self.frames = []
        self.numFrames = 0
        
    def has_capacity(self):
        if self.numFrames == FRAMECOUNT:
            return True
        return false
    
    #Select LRU(or whatever method), send to disk
    def evict_page(self):
        
    #Bring page in, if full, evict a page first
    def load_page(self):
        
    #Write all pages to disk
    def close(self):
        
    
class Frame:

    def __init__(self, path):
        
        self.dirtyBit = False
        self.pin = False
        self.path = path
        
    def set_dirty_bit(self):
        if dirtyBit == True:
            dirtyBit = False
        else:
            dirtyBit = True
        
    def set_pit(self):
        if pin == True:
            pin = False
        else:
            pin = True