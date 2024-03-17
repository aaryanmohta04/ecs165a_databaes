class lockEntry:

    def __init__(self, rid, lock):
        self.rid = rid
        self.lockInfo = lock

class lock_manager:
    def __init__(self):
        self.manager = []
        self.curIndex = 0

    def insert(self, rid, lock):
        self.manager.append(lockEntry(rid, lock))

    def search(self, rid):
        for i in range(len(self.manager)):
            if self.manager[i].rid == rid:
                self.curIndex = i
                return self.manager[i].lock
        return False
    
    def _search(self, rid):
        for i in range(len(self.manager)):
            if self.manager[i].rid == rid:
                return self.manager[i]
        return False
        
    # def getIndex(self, rid):
    #     for i in range(len(self.manager)):
    #         if self.manager[i].rid == rid:
    #             return i
    #     return False
        
    def remove(self, rid):
        curNode = self._search(rid)

        if curNode != False:
            self.manager.remove(curNode)


