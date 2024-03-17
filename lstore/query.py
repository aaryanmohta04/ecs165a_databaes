from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import lock
from datetime import datetime
import numpy as np
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
        primary_key_column = 0 
        rid = self.table.index.locate(primary_key_column, primary_key)
        rid = self.table.page_directory[rid]
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
        newrid = []
        key_directory = (rid[0], rid[1], 'b')
        self.table.bufferpool.frames[frame_index].indirection[rid[2]] = [0,0,0, 'd']
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, rollback=False):
        if rollback == True:
            print("rollback")
            return True

        else:
            start_time = datetime.now().strftime("%Y%m%d%H%M%S")
            schema_encoding = '0' * self.table.num_columns  #add '0000...' for schema_encoding
            self.table.insertRec(start_time, schema_encoding, *columns) #call function in Table.py to insert record
        return True
        

    # For select, gives only desired columns
    def modify_columns(self, record, projected_columns_index):
        new_record = []
        for i in range(len(record.columns)):
            if projected_columns_index[i] == 1:
                new_record.append(record.columns[i])
        
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
        # if self.table.index.has_index(search_key_index) == False:
        #     self.table.index.create_index(search_key_index)
        
        rid = self.table.index.locate(search_key_index, search_key)
        records = []
        # for rid in rids:
        # rid = self.table.page_directory[rid]
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
        newrid = []
        key_directory = (rid[0], rid[1], 'b')
        newrid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        if (newrid == [0,0,0,'d']):
            print("error, tried selecting deleted record")
        rid = newrid
        # TPS = (self.table.bufferpool.frames[frame_index].TPS[rid[2]])
        # print(TPS)
        TPS = [0,0]
        record = self.table.find_record(search_key, rid, projected_columns_index, TPS)
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
            rid = self.table.index.locate(search_key_index, search_key)
        # for rid in rids: 
            baseRID = rid
            frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
            rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]] # converts base page rid to tail rid if any, else remains same  
            while relative_version != 0: 
                if(rid[3] == 'b'):
                    if(tuple(rid) != baseRID):
                        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                else: 
                    frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                    rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                relative_version += 1
            records = []
        # for rid in rids:
            record = self.table.find_record(search_key, rid, projected_columns_index, [0,0])
            records.append(record)
            return records
            pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, rollback=False):
        if(rollback == True):
            self.rollBackUpdate(primary_key, *columns)
        else:
            rid = self.table.index.locate(self.table.key, primary_key) #gets rid using key in index
            BaseRID = rid #sets baseRID to the rid found with key
            #oldRID = rid looks like it's the same as currentRID? so probably not needed
            rid = self.table.page_directory[rid] #sets rid to the physical location of record using page_directory
            self.table.updateRec(rid, BaseRID, primary_key, *columns)
            return True
        pass
    
    def rollBackUpdate(self, primary_key, *columns):
        rid = self.table.index.locate(0, primary_key)
        baseRID = rid
        rollBackRID = rid
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        if(rid[3] == 'b'):
            if(tuple(rid) != baseRID):
                frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                rollBackRID = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        else: 
            frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns, self.table.name)
            rollBackRID = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        
        frame_index = self.table.bufferpool.load_base_page(baseRID[0], baseRID[1], self.table.num_columns, self.table.name)
        self.table.bufferpool.frames[frame_index].indirection[baseRID[2]] = rollBackRID 
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
        rids = self.table.index.locate_range(start_range,end_range, aggregate_column_index)
        sum = 0
        for rid in rids:
            rid = self.table.page_directory[rid]
            frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
            key_directory = (rid[0], rid[1], 'b')
            indirectrid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
            if indirectrid[3] == 't':
                if(self.table.greaterthan(self.table.bufferpool.extractTPS(key_directory, self.table.num_columns), [indirectrid[1], indirectrid[2]])):
                    data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, rid[2])
                else:
                    self.table.bufferpool.load_tail_page(indirectrid[0], indirectrid[1], self.table.num_columns, self.table.name)
                    key_directory = (indirectrid[0], indirectrid[1], 't')
                    data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, indirectrid[2])
                
            if(rid[3] == 'b'):
                data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, rid[2]) 
            sum += data[aggregate_column_index]

        return sum

    
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
        rids = self.table.index.locate_range(start_range,end_range, aggregate_column_index)
        sum = 0
        records = []
        relative_version_copy = relative_version
        for rid in rids: 
            relative_version = relative_version_copy
            baseRID = rid
            frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
            rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]] # converts base page rid to tail rid if any, else remains same  
            while relative_version != 0: 
                if(rid[3] == 'b'):
                    if(tuple(rid) != baseRID):
                        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                else: 
                    frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                    rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                relative_version += 1
            record = self.table.find_record(0, rid, [1,1,1,1,1], [0,0])
            records.append(record)
        for record in records: 
            sum += record.columns[aggregate_column_index]
        return sum
        pass

    
    """
    increments one column of the record
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
