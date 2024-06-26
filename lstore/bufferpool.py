from asyncio.windows_events import NULL
from pickle import FALSE, TRUE
# from lstore.table import Table
from lstore.page import Page
from datetime import datetime
import os

# How many?
FRAMECOUNT = 100
#need to make table directory with all tables and relevent table values with pickle

class Bufferpool:
    
    def __init__(self, path_to_root):
        
        self.frames = []
        self.numFrames = 0
        self.path_to_root = path_to_root
        self.current_table_path = None #to keep track of current table, we're working in
        self.current_total_path = None
        self.frame_directory = [] #put 
        self.frame_info = [None] * 100

    def has_capacity(self):
        if self.numFrames < FRAMECOUNT:
            return True
        return False
    
    def start_db_dir(self):
        if os.path.isdir(self.path_to_root) == FALSE: #if path doesn't exist
            os.mkdir(self.path_to_root) #create it (./ECS165) (will we need parent path i.e. /home/user etc?)
        pass

    def start_table_dir(self, name, numCols): #makes directory for table within db directory
        path = self.path_to_root + f"/tables/"
        path = os.path.join(path, name)
        if not os.path.exists(path):
            os.mkdir(path)
        self.current_table_path = path

    def allocate_page_range(self, numCols, pageRangeIndex): #allocates enough space for 16 BPs in new page range
        page_range_path = f"{self.current_table_path}/pageRange{pageRangeIndex}"
        os.mkdir(page_range_path)

        for i in range(16):
            bp_file_location = f"{page_range_path}/basePage{i}.bin"
            bp_file = open(bp_file_location, "wb")

            # for j in range(numCols + 11): #number of columns + rid(4), indirection, time, schema (need base rid column too?), metadata: TPS, numRecords (one page with metadata single elements)
            #     bp_file.write(bytearray(4096))

            bp_file.close()

        tail_page_path = f"{page_range_path}/tailPages"
        os.mkdir(tail_page_path)

    def allocate_tail_page(self, numCols, pageRangeIndex, numTPs):

        tp_file_location = f"{self.current_table_path}/pageRange{pageRangeIndex}/tailPages/tailPage{numTPs}.bin"

        if not os.path.exists(tp_file_location):
            tp_file = open(tp_file_location, "wb")

            for i in range(numCols + 14): #number of cols + rid(4), indirection, schema, base rid, metadata: numRecords
                tp_file.write(bytearray(4096))
            
            tp_file.close()

        
        

    def get_table_dir(self, name): #sets the current path to look into current table directory
        path = os.path.join(self.path_to_root, name)
        if os.path.isdir(path):
            self.current_table_path = path

    def extractRID(self, key_directory, num_columns, recordnumber): 
        newrid = []
        frame_index = self.get_frame_index(key_directory)
        cur_frame = self.frames[frame_index]
        for i in range(3):
            x = int.from_bytes((cur_frame.frameData)[num_columns + i].data[recordnumber*8:(recordnumber + 1)*8], 'big')
            newrid.append(x)
        return newrid
    
    def extractdata(self, frame_index, num_columns, recordnumber): 
        data = []
        cur_frame = self.frames[frame_index]
        for i in range(num_columns):
            #frame_index = self.frame_directory[key_directory]
            x = int.from_bytes((cur_frame.frameData)[i].data[recordnumber*8:(recordnumber + 1)*8], 'big')
            data.append(x)
        return data
    
    def extractIndirection(self, key_directory, num_columns, recordnumber): 
        newrid = []
        frame_index = self.get_frame_index(key_directory)
        cur_frame = self.frames[frame_index]
        for i in range(3):
            #frame_index = self.frame_directory[key_directory]
            x = int.from_bytes((cur_frame.frameData)[num_columns + i + 4].data[recordnumber*8:(recordnumber + 1)*8], 'big')
            newrid.append(x)
        return newrid
    
    def extractBaseRID(self,frame_index, num_columns, recordnumber): 
        newrid = []
        cur_frame = self.frames[frame_index]
        for i in range(3):
            #frame_index = self.frame_directory[key_directory]
            x = int.from_bytes((cur_frame.frameData)[num_columns + i + 8].data[recordnumber*8:(recordnumber + 1)*8], 'big')
            newrid.append(x)
        return newrid

    def extractTPS(self, key_directory, num_columns):
        #frame_index = self.frame_directory[key_directory]
        frame_index = self.get_frame_index(key_directory)
        cur_frame = self.frames[frame_index]
        if(len(((self.frames[frame_index]).frameData)) >= num_columns + 11):
            x = int.from_bytes((cur_frame.frameData)[num_columns + 10].data[0:8], 'big')
            y = int.from_bytes((cur_frame.frameData)[num_columns + 10].data[8:16], 'big')
        else: 
            x = y = 0
        return [x,y]
    
    def extractRecordCount(self, key_directory, num_columns):
        x = 0
        frame_index = self.get_frame_index(key_directory)
        cur_frame = self.frames[frame_index]
        if key_directory[2] == 'b':
            if(len(((self.frames[frame_index]).frameData)) >= num_columns + 11):
                x = int.from_bytes((cur_frame.frameData)[num_columns + 10].data[16:24], 'big')
        elif key_directory[2] == 't':
            if(len(((self.frames[frame_index]).frameData)) >= num_columns + 14):
                x = int.from_bytes((cur_frame.frameData)[num_columns + 13].data[0:8], 'big')
        
        return x
    
#use frame directory
    def in_pool(self, key):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key:
                return True
        return False
            
    def get_frame_index(self, key_directory):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key_directory:
                return i
         
    def LRU(self):
        evict_index = 0
        for i in range(len(self.frames) - 1):
            if self.frames[i].lastAccess > self.frames[i + 1].lastAccess:
                if self.frames[i + 1].isPinned() == FALSE:
                    evict_index = i + 1
        return evict_index
    
    def evict_page(self):
        evict_index = self.LRU()
        evict_frame = self.frames[evict_index]
        frame_size = len(evict_frame.frameData)
        if(evict_frame.dirtyBit == TRUE):
            #self.frames[evict_index].write_to_disk(self.current_total_path, self.frames[evict_index].frameData) #writes data to disk
            evict_frame.pin_page()
            
            if self.frame_info[evict_index][2] == 'b':
                    path = f"{self.path_to_root}/pageRange{self.frame_info[evict_index][0]}/basePage{self.frame_info[evict_index][1]}.bin"
                    self.write_start_time(path, frame_size + 9, evict_index)
                    #self.write_TPS(path, frame_size + 10)
                    self.write_numRecords(path, frame_size + 10, evict_index, 0)

            elif self.frame_info[evict_index][2] == 't':
                    path = f"{self.path_to_root}/pageRange{self.frame_info[evict_index][0]}/tailPages/tailPage{self.frame_info[evict_index][1]}.bin"
                    self.write_baseRid(path, frame_size + 9, evict_index)
                    self.write_numRecords(path, frame_size + 13, evict_index, 0)
                    
            for j in range(frame_size):
                evict_frame.frameData[j].write_to_disk(path, evict_frame.frameData[j].data, j)
            self.write_rid(path, frame_size, evict_index)
            self.write_indirection(path, frame_size + 4, evict_index)
            self.write_schema_encoding(path, frame_size + 8, evict_index)
                
            evict_frame.unpin_page()
        return evict_index
        #I'm thinking that if it's not dirty, we can just write over the info when we load, so we can just leave it and return which index it's at

    def get_empty_frame(self, path, numColumns):
        if not self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(path, numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(path, numColumns))
            self.numFrames += 1
        return frame_index

    #Bring page in from disk, if full, evict a page first
    def load_base_page(self, page_range_index, base_page_index, numColumns, table_name):
        path_to_page = self.current_table_path + f"/pageRange{page_range_index}/basePage{base_page_index}.bin"
        self.current_total_path = path_to_page
        d_key = (page_range_index, base_page_index, 'b')
        if self.in_pool(d_key):
            return self.get_frame_index(d_key)

        frame_index = self.get_empty_frame(path_to_page, numColumns)
        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()
        self.frame_info[frame_index] = d_key
        for i in range(numColumns):
            cur_frame.frameData[i] = Page()
            cur_frame.frameData[i].read_from_disk(path_to_page, i) #read data from page into frame
        
            
        self.load_meta_data(path_to_page, numColumns, frame_index, 'b')
            
        #try:
        #    cur_frame.numRecords = self.extractRecordCount(directory_key, numColumns)
        #except:
        #    cur_frame.numRecords = 0

        #if not cur_frame.numRecords == 0:
            #self.frame_info[frame_index] = directory_key (done in line 206)
        #    cur_frame.TPS = self.extractTPS(directory_key, numColumns)
            #self.frames[frame_index].numRecords = self.extractRecordCount(directory_key, numColumns) (should've been done already if it wasn't error)
        #    for i in range(cur_frame.numRecords):
        #        cur_frame.rid[i] = self.extractRID(directory_key, numColumns, 0)
        #        cur_frame.indirection[i] = self.extractIndirection(directory_key, numColumns, 0)

        cur_frame.unpin_page()

        return frame_index

        pass
    
    #File Structure:
    # 1.) Pages (num_cols * 512 records of data): col1 -> col2 -> coln
    # 2.) METADATA: numRecords -> RID * 512 -> Indirection * 512 -> schema * 512
    # 3.) B vs T: If B: -> TPS  If T: -> BaseRID
    
    
    # def load_meta_data(self, path, numColumns, frame_index, page_type):
    #     index = numColumns * 4096
    #     maxPages = 512
    #     file = open(path, "rb")
    #     file.seek(index)
    #     self.frames[frame_index].numRecords = int.from_bytes(file.read(8), 'big')
    #     if self.frames[frame_index].numRecords == 0:
    #         return
    #     for i in maxPages:
    #         self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
    #     for i in maxPages:
    #         self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
    #     for i in maxPages:
    #         self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
    #     if page_type == 't':
    #         for i in maxPages:
    #             self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
    #     else:
    #         for i in maxPages:
    #             self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
    #     file.close()
        

    def load_meta_data(self, path, numColumns, frame_index, page_type):
        index = 0
        if(page_type == 'b'):
            index = (numColumns + 10) * 4096           
        else:
            index = (numColumns + 13) * 4096
        file = open(path, "rb")
        file.seek(index)
        self.frames[frame_index].numRecords = int.from_bytes(file.read(8), 'big')
        # reset file pointer
        if self.frames[frame_index].numRecords == 0:
            return
        # read rids
        newRID = []
        for i in range(4):
            file.seek(0,0)
            file.seek((numColumns+ i) * 4096)
            for j in range(self.frames[frame_index].numRecords):
                if(i == 0):
                    newRID.append([int.from_bytes(file.read(8), 'big')])
                elif( i == 3):
                    if(int.from_bytes(file.read(8), 'big') == 0):
                        newRID[j].append('b')
                    else:
                        newRID[j].append('t')
                else: 
                    newRID[j].append(int.from_bytes(file.read(8), 'big'))
        self.frames[frame_index].rid = newRID
        #read Indirection
        newIndirection = []
        for i in range(4):
            file.seek(0,0)
            file.seek((numColumns + 4 + i) * 4096)
            for j in range(self.frames[frame_index].numRecords):
                if(i == 0):
                    newIndirection.append([int.from_bytes(file.read(8), 'big')])
                elif( i == 3):
                    if(int.from_bytes(file.read(8), 'big') == 0):
                        newIndirection[j].append('b')
                    else:
                        newIndirection[j].append('t')
                else: 
                    newIndirection[j].append(int.from_bytes(file.read(8), 'big'))
        self.frames[frame_index].indirection = newIndirection
        #read schema_encoding
        tempStr = ''
        tempLen = 0
        newStr = ''
        newSchema = []
        file.seek(0,0)
        file.seek((numColumns + 8) * 4096)
        for j in range(self.frames[frame_index].numRecords):
            tempStr = str(int.from_bytes(file.read(8), 'big'))
            tempLen = len(tempStr)
            tempLen = numColumns - tempLen
            for i in range(tempLen):
                newStr = newStr + '0'
            newStr = newStr + tempStr
            newSchema.append(newStr)
            newStr = ''
        self.frames[frame_index].schema_encoding = newSchema
        #read start_time
        if(page_type == 'b'):
            file.seek(0,0)
            file.seek((numColumns + 9) * 4096)
            for j in range(self.frames[frame_index].numRecords):
                self.frames[frame_index].start_time.append(int.from_bytes(file.read(8), 'big'))
        elif(page_type == 't'):
            #read baseRID
            newBaseRID = []
            for i in range(4):
                file.seek(0,0)
                file.seek((numColumns + 9 + i) * 4096)
                for j in range(self.frames[frame_index].numRecords):
                    if(i == 0):
                        newBaseRID.append([int.from_bytes(file.read(8), 'big')])
                    elif( i == 3):
                        if(int.from_bytes(file.read(8), 'big') == 0):
                            newBaseRID[j].append('b')
                        else:
                            newBaseRID[j].append('t')
                    else: 
                        newBaseRID[j].append(int.from_bytes(file.read(8), 'big'))
            self.frames[frame_index].BaseRID = newBaseRID

        # self.frames[frame_index].indirection = newIndirection
                
            #self.frames[frame_index].rid.append(int.from_bytes(file.read(8*i), 'big'))
        # for i in maxPages:
        #     self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
        # for i in maxPages:
        #     self.frames[frame_index].append(int.from_bytes(file.read(8*i), '(page_big'))
        # if page_type == 't':
        #     for i in maxPages:
        #         self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
        # else:
        #     for i in maxPages:
        #         self.frames[frame_index].append(int.from_bytes(file.read(8*i), 'big'))
        file.close()

    # def write_meta_data(self, path, numColumns, frame_index, page_type):
    #     index = numColumns * 4096
    #     maxPages = 512
    #     file = open(path, "rb+")
    #     file.seek(index)
    #     file.write((self.frames[frame_index].numColumns).to_bytes(8, 'big'))
    #     # RID
    #     for i in maxPages:
    #         self.write_RID(file, self.frames[frame_index].rid, i)
    #     # Indirection
    #     for i in maxPages:
    #         self.write_RID(file, self.frames[frame_index].indirection, i)
    #     # Schema
    #     for i in maxPages:
    #         file.write(int(self.frames[frame_index].schema_encoding[i]).to_bytes(8, 'big'))
    #     if page_type == 't':
    #     # BaseRID
    #         for i in maxPages:
    #             self.write_RID(file, self.frames[frame_index].BaseRID, i)
    #     else:
    #     # TPS
    #         for i in maxPages:
    #             file.write((self.frames[frame_index].TPS[i]).to_bytes(8, 'big'))
    #     file.close()
        
    # def read_RID(self, file, rid, i):
    #     for j in range(3):
    #             rid[i].append(int.from_bytes(file.read(8), 'big'))
    #     file_type = int.from_bytes(file.read(1), 'big')
    #     if file_type == '0':
    #         rid[i].append('t')
    #     else:
    #         rid[i].append('b')
        
    # def write_RID(self, file, rid, i):
    #     for j in range(3):
    #             file.write((rid[i][j]).to_bytes(8, 'big'))
    #     if rid[i][3] == 't':
    #         file.write((0).to_bytes(1, 'big'))
    #     else:
    #         file.write((1).to_bytes(1, 'big'))
         
        

    def load_tail_page(self, page_range_index, tail_page_index, numColumns, table_name):
        path_to_page = self.current_table_path + f"/pageRange{page_range_index}/tailPages/tailPage{tail_page_index}.bin"
        self.current_total_path = path_to_page
        self.allocate_tail_page(numColumns, page_range_index, tail_page_index) #will check if the tail page exists already, and if not, create it
        d_key = (page_range_index, tail_page_index, 't')
        if self.in_pool(d_key):
            return self.get_frame_index(d_key)

        frame_index = self.get_empty_frame(path_to_page, numColumns)
        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()
        self.frame_info[frame_index] = d_key
            
        for i in range(numColumns):
            cur_frame.frameData[i] = Page()
            cur_frame.frameData[i].read_from_disk(path_to_page, i) #read data from page into frame
        
        self.load_meta_data(path_to_page, numColumns, frame_index, 't')

        #try:
        #    cur_frame.numRecords = self.extractRecordCount(directory_key, numColumns)
        #except:
        #    cur_frame.numRecords = 0

        #if not cur_frame.numRecords == 0:
            #self.frames[frame_index].numRecords = self.extractRecordCount(directory_key, numColumns) (should've been done already if it didn't cause error)
        #    for i in range(self.frames[frame_index.numRecords]):
        #        cur_frame.rid[i] = self.extractRID(directory_key, numColumns, 0)
        #        cur_frame.indirection[i] = self.extractIndirection(directory_key, numColumns, 0)
        #        cur_frame.BaseRID[i] = self.extractBaseRID(directory_key, numColumns, 0)
        
        cur_frame.unpin_page()

        return frame_index
            
    

    def insertRecBP(self, RID, start_time, schema_encoding, indirection, *columns, numColumns):
        key_directory = (RID[0], RID[1], 'b')
        frame_index = self.get_frame_index(key_directory=key_directory)
        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()
        for i in range(numColumns): #iterates through number of columns and writes data in *columns to corresponding page in page[] 
            cur_frame.frameData[i].write(columns[i])
        cur_frame.numRecords += 1
        #print("numRecords: " + str(self.frames[frame_index].numRecords))
        #print(str(self.frames[frame_index].has_capacity()))
        cur_frame.rid.append(RID)
        cur_frame.start_time.append(start_time)
        cur_frame.schema_encoding.append(schema_encoding)
        cur_frame.indirection.append(indirection)
        #print(self.frames[frame_index].frameData[0].get_value(self.frames[frame_index].numRecords - 1))
        cur_frame.set_dirty_bit()
        cur_frame.unpin_page()

    def insertRecTP(self, record, rid, updateRID, currentRID, baseRID, curFrameIndexBP, *columns):
        key_directory = (updateRID[0], updateRID[1], 't')
        frame_index = self.get_frame_index(key_directory=key_directory)
        self.frames[frame_index].pin_page()
        self.frames[curFrameIndexBP].pin_page()

        schema = '' #aded schema encoding here because it's where I iterate through the data anyway
        for j in range(len(columns)):
            if columns[j] != None:
                self.frames[frame_index].frameData[j].write(columns[j])
                schema = schema + '1'

                self.frames[curFrameIndexBP].schema_encoding[j] = 1
            else:
                self.frames[frame_index].frameData[j].write(record.columns[j])
                schema = schema + '0'

        self.frames[frame_index].schema_encoding.append(schema) #puts the schema encoding in
        self.frames[frame_index].numRecords += 1

        self.frames[frame_index].indirection.append(currentRID)
        self.frames[frame_index].BaseRID.append(baseRID)
        self.frames[frame_index].rid.append(updateRID)

        self.frames[frame_index].unpin_page()
      
        self.frames[curFrameIndexBP].indirection[rid[2]] = updateRID #set basePage indirection to new tail record rid

        self.frames[curFrameIndexBP].unpin_page()

    def deleteRec(self, rid, curFrameIndex, numCols):
        self.frames[curFrameIndex].pin_page()

        for i in range(numCols):
             self.frames[curFrameIndex].frameData[i].num_records -= 1
             #print('column: ' + str(i) + 'numRec: ' + str(self.frames[curFrameIndex].frameData[i].num_records))
        #don't need to do anything with the actual data in frame cause it should just be overwritten with the next insert since we're resetting the indexes back to what they were before the insert

        self.frames[curFrameIndex].rid.pop(rid[2])
        self.frames[curFrameIndex].indirection.pop(rid[2])
        self.frames[curFrameIndex].schema_encoding.pop(rid[2])

        self.frames[curFrameIndex].numRecords -= 1

        #print('numRecs:' + str(self.frames[curFrameIndex].numRecords))
        #print('test:' + str(len(self.frames[curFrameIndex].rid)))

        if(rid[3] == 'b'):
            self.frames[curFrameIndex].start_time.pop(rid[2])
        elif(rid[3] == 't'):
            self.frames[curFrameIndex].BaseRID.pop(rid[2])

        self.frames[curFrameIndex].unpin_page()

    
    #Write all pages to disk
    def write_rid(self, path, numCols, curIndex): #curIndex refers to current BP index
        for i in range(4):
            tempPage = Page()
            for j in range(self.frames[curIndex].numRecords):
                if(self.frames[curIndex].rid[j][i] == 'b'): 
                    tempPage.write((0))
                elif(self.frames[curIndex].rid[j][i] == 't'):
                    tempPage.write((1))
                else: 
                    tempPage.write(self.frames[curIndex].rid[j][i]) 
            tempPage.write_to_disk(path, tempPage.data, numCols + i)
            
    def write_indirection(self, path, numCols, curIndex):
        for i in range(4):
            tempPage = Page()
            for j in range(self.frames[curIndex].numRecords):
                if self.frames[curIndex].indirection[j][i] == 'b':
                    tempPage.write(0)
                elif self.frames[curIndex].indirection[j][i] == 't':
                    tempPage.write(1)
                elif self.frames[curIndex].indirection[j][i] == 'd':
                    tempPage.write(2)
                else:
                    tempPage.write(self.frames[curIndex].indirection[j][i])

            tempPage.write_to_disk(path, tempPage.data, numCols + i)

    def write_baseRid(self, path, numCols, curIndex):
        for i in range(4):
            tempPage = Page()
            for j in range(self.frames[curIndex].numRecords):
                if(self.frames[curIndex].BaseRID[j][i] == 'b'):
                    tempPage.write(0)
                elif(self.frames[curIndex].BaseRID[j][i] == 't'):
                    tempPage.write(1)
                else:
                    tempPage.write(self.frames[curIndex].BaseRID[j][i])
            
            tempPage.write_to_disk(path, tempPage.data, numCols + i)

    def write_start_time(self, path, numCols, curIndex):
        tempPage = Page()
        for j in range(self.frames[curIndex].numRecords):
            tempPage.write(self.frames[curIndex].start_time[j])
        tempPage.write_to_disk(path, tempPage.data, numCols)


    def write_schema_encoding(self, path, numCols, curIndex):
        tempPage = Page()
        for j in range(self.frames[curIndex].numRecords):
            tempPage.write(int(self.frames[curIndex].schema_encoding[j]))

        tempPage.write_to_disk(path, tempPage.data, numCols)

    def write_TPS(self, path, numCols, curIndex):
        for i in range(2):
            tempPage = Page()
            tempPage.write(self.frames[curIndex].TPS[i])
            tempPage.write_to_disk_record(path, tempPage.data, numCols, i)

    def write_numRecords(self, path, numCols, curIndex):
        tempPage = Page()
        tempPage.write(self.frames[curIndex].numRecords)
        tempPage.write_to_disk(path, tempPage.data, numCols)

    def close(self, tablename):
        print(f"closing {tablename}")
        for i in range(len(self.frames)):
            frame_size = len(self.frames[i].frameData)
        # if self.frames[i].dirtyBit == True:
            if self.frame_info[i][2] == 'b':
                path = f"{self.path_to_root}/tables/{tablename}/pageRange{self.frame_info[i][0]}/basePage{self.frame_info[i][1]}.bin" 
                self.write_start_time(path, frame_size + 9, i)
                # self.write_TPS(path, frame_size + 10, i)
                self.write_numRecords(path, frame_size + 10, i)

            elif self.frame_info[i][2] == 't':
                path = f"{self.path_to_root}/tables/{tablename}/pageRange{self.frame_info[i][0]}/tailPages/tailPage{self.frame_info[i][1]}.bin"
                self.write_baseRid(path, frame_size + 9, i)
                self.write_numRecords(path, frame_size + 13, i)
                
            for j in range(frame_size):
                self.frames[i].frameData[j].write_to_disk(path, self.frames[i].frameData[j].data, j)
            self.write_rid(path, frame_size, i)
            self.write_indirection(path, frame_size + 4, i)
            self.write_schema_encoding(path, frame_size + 8, i)
        pass

class Frame:

    def __init__(self, path, numColumns):
        self.frameData = [None] * numColumns #like pages[]
        self.TPS = [0,0]
        self.numRecords = 0
        self.rid = []
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.BaseRID = []
        self.dirtyBit = False
        self.pinNum = 0
        self.path = path
        self.columns = [None] * numColumns
        self.lastAccess = 0
        self.numColumns = numColumns

    def has_capacity(self):
        if self.numRecords < 512:
            return TRUE
        else: 
            return FALSE
        
    def set_dirty_bit(self):
        if self.dirtyBit == True:
            self.dirtyBit = False
        else:
            self.dirtyBit = True
        
    def pin_page(self):
        self.pinNum += 1
        self.lastAccess = datetime.now()

    def unpin_page(self):
        self.pinNum -= 1

    def isPinned(self):
        if self.pinNum == 0:
            return False
        else:
            return True
