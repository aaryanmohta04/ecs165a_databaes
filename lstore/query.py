from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = 
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        start_time = datetime.now().strftime("%Y%m%d%H%M%S")
        schema_encoding = '0' * self.table.num_columns  #add '0000...' for schema_encoding
        self.table.insertRec(*columns, start_time, schema_encoding) #call function in Table.py to insert record
        

        return True
        
        pass

    # For select, gives only desired columns
    def modify_columns(record, projected_columns_index):
        new_record = []
        for i in range(len(record.columns)):
            if projected_columns_index[i] == 1:
                new_record.append[record.columns[i]]
        
        return new_record
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        if self.table.index.has_index(search_key_index) == False:
            self.table.index.create_index(search_key_index)
        
        rids = self.table.index.locate(search_key_index, search_key)
        records = []
            
        for rid in rids:
            rid = self.pageRange[rid[0]].basePages[rid[1]].indirection[rid[3]]
            record = self.table.find_record(rid, projected_columns_index)
            records.append(record)
        return records
        pass
    

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rids = self.table.index.locate(search_key_index, search_key)
        for rid in rids: 
            rid = self.pageRange[rid[0]].basePages[rid[1]].indirection[rid[3]] # converts base page rid to tail rid if any, else remains same
            while relative_version != 0: 
                rid = self.pageRange[rid[0]].basePages[rid[1]].indirection[rid[3]]
                relative_version += 1
            records = []
            
        for rid in rids:
            record = self.table.find_record(rid, projected_columns_index)
            records.append(record)
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        rid = self.table.index.locate(self.table.key, primary_key)
        rid = self.pageRange[rid[0]].basePages[rid[1]].indirection[rid[3]]
        #need the RID somehow (index?)
        #use the key or RID to get the right record in Table.py
        #in Table.py, can check if tail record is full or needs to be made
        #can then call insertRecTP

        #will have to update tailPage (schema, indirection, records)
        #will have to update base page record (schema, indirection)
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        rids = table.index.locate_range(start_range,end_range, aggregate_column_index)
        for rid in rids:
            rid = self.pageRange[rid[0]].basePages[rid[1]].indirection[rid[3]]
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):

        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
