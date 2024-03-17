from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        self.table = table
        self.rollback = []
        self.queryIndex = 0
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
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

    
    def abort(self):
        for i in range(self.queryIndex):
            if self.rollback[i] == True:
                self.queries[i][0](*self.queries[i][1], rollback=True)
        return False

    
    def commit(self):
        return True

