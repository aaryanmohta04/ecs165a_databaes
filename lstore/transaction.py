from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.keys = []
        self.key_index = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table=Table, *args):
        self.queries.append((query, args))
        self.table = table
        self.rollback = []
        self.queryIndex = 0
        self.type = 'I'
        # use grades_table for aborting

    def add_query(self, query, table, key, *args):
        self.queries.append((query, args))
        self.table = table
        self.rollback = []
        self.queryIndex = 0
        self.keys.append(key)
        self.type = 'U'
        pass

    def add_query(self, query, table, key, key_index, *args):
        self.queries.append((query, args))
        self.table = table
        self.rollback = []
        self.queryIndex = 0
        self.keys.append(key)
        self.key_index.append(key_index)
        self.type = 'S'
        pass

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        if self.type == 'I':
            for query, args in self.queries:
                result = query(*args)
                if result == True:
                    self.rollback.append(True)
                    #print(self.queries[self.queryIndex][0])
                self.queryIndex += 1
                # If the query has failed the transaction should abort
                if result == False:
                    return self.abort()
            return self.commit()
        
        elif self.type == 'U':
            for query, args in self.queries:
                result = query(self.keys[self.queryIndex], *args)
                if result == True:
                    self.rollback.append(True)
                    #print(self.queries[self.queryIndex][0])
                self.queryIndex += 1
                # If the query has failed the transaction should abort
                if result == False:
                    return self.abort()
            return self.commit()

        elif self.type == 'S':
            pass

    
    def abort(self):
        if self.type == 'I':
            for i in reversed(range(self.queryIndex)):
                if self.rollback[i] == True:
                    self.queries[i][0](*self.queries[i][1], rollback=True)

        elif self.type == 'U':
            for i in reversed(range(self.queryIndex)):
                if self.rollback[i] == True:
                    self.queries[i][0](self.keys[i], *self.queries[i][1], rollback=True)

        elif self.type == 'S':
            pass
        return False

    
    def commit(self):
        # for i in reversed(range(self.queryIndex)):
        #     if self.rollback[i] == True:
        #         self.queries[i][0](*self.queries[i][1], rollback=True)
        return True

