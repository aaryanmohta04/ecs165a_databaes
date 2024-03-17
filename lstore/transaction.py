from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        #self.keys = []
        #self.key_index = []
        self.type = []
        self.rollback = []
        self.queryIndex = 0
        #self.numQueries = 0
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
        #self.keys.append(None)
        #self.key_index.append(None)
        self.type.append(query.__name__)
        #self.numQueries += 1
        # use grades_table for aborting

    # def add_query(self, query, table, key, *args):
    #     self.queries.append((query, args))
    #     self.table = table
    #     self.keys.append(key)
    #     self.key_index.append(None)
    #     self.type[self.numQueries] = 'U'
    #     self.numQueries += 1
    #     pass

    # def add_query(self, query, table, key, key_index, *args):
    #     self.queries.append((query, args))
    #     self.table = table
    #     self.keys.append(key)
    #     self.key_index.append(key_index)
    #     self.type[self.numQueries] = 'S'
    #     self.numQueries += 1
    #     pass

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            if result == True or result != None:
                self.rollback.append(True)
                #print(self.queries[self.queryIndex][0])
            self.queryIndex += 1
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()
    
    def abort(self):
        for i in reversed(range(self.queryIndex)):
            if self.rollback[i] == True:
                self.queries[i][0](*self.queries[i][1], rollback=True)
        return False

    
    def commit(self):
        for i in reversed(range(self.queryIndex)):
            self.queries[i][0](*self.queries[i][1], rollback=False, commit=True)

        # for i in reversed(range(self.queryIndex)):
        #     if self.rollback[i] == True:
        #         self.queries[i][0](*self.queries[i][1], rollback=True)
        return True

