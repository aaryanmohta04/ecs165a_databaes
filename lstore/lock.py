from lstore.lock_manager import lockEntry, lock_manager

import threading

class lock:

    def __init__(self):
        self.numRLock = 0
        self.numWLock = 0
        self.mutex = threading.Lock()
        pass

    def canRLock(self):
        if self.numWLock == 0:
            self.mutex.acquire()
            self.numRLock += 1
            return True
        else: 
            return False
    
    def canWLock(self):
        if self.numRLock == 0 and self.numWLock == 0:
            self.mutex.acquire()
            self.numWLock += 1
            return True
        else:
            return False
        
    def releaseRLock(self):
        if self.numRLock > 0:
            self.mutex.release()
            self.numRLock -= 1

    def releaseWLock(self):
        if self.numWLock > 0:
            self.mutex.release()
            self.numWLock -= 1
        