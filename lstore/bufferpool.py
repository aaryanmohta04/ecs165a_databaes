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