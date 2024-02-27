# UPDATE IN TESTING
#
# Following example in milestone1:
# PAGE_SIZE = 4096 bytes
# Base Page/ tail page has 1 page per column
# Page Range has 16 base pages + any tail pages
# Number of records per base page = 512


#for arrays and stuff
import numpy as np

MAX_RECORDS_PER_PAGE = 512
MAX_BASEPAGES_PER_RANGE = 16

#One Page for Every Column in Table (maybe 4k pages/columns per base page)
class Page:

    def __init__(self):
        self.num_records = 0
        # 4096 bytes can hold 512 records
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records < MAX_RECORDS_PER_PAGE:
            return True
        else:
            return False

    def write(self, value):
        # 8 bytes per record
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1
        
    def find_value(self,value):
        indexes = []
        for i in range(MAX_RECORDS_PER_PAGE):
            cur_val = int.from_bytes(self.data[i*8:(i+1)*8], byteorder = 'big')
            if cur_val == value:
                indexes.append(i)
        return indexes
        
    def get_value(self, index):
        val = int.from_bytes(self.data[index*8:(index + 1)*8], 'big')
        return val

#One Base_Page Contains many pages/columns (16 BPs in Page Range)
#Technically Tail Pages also create 4k columnar pages (could this class be used for both?)
class BasePage:

    #need to know how many records in BP (4k is max)
    #need 4k pages/columns per BP
    #need to create an array of pages?
    #creating 4k columnar pages for each BP
    def __init__(self, numCols):
        self.rid = [None] * 512
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.pages = []
        self.num_records = 0
        self.num_cols = numCols

        #create array of pages/cols (4 allocated for schema, indirection, rid, start time)
        for i in range(numCols):
            self.pages.append(Page())

    def has_capacity(self):
        if self.num_records < MAX_RECORDS_PER_PAGE:
            return True
        else:
            return False
            
    def insertRecBP(self, RID, start_time, schema_encoding, indirection, *columns):
        for i in range(self.num_cols): #iterates through number of columns and writes data in *columns to corresponding page in page[] 
            self.pages[i].write(columns[i])
        self.num_records += 1
        self.rid.append(RID)
        self.start_time.append(start_time)
        self.schema_encoding.append(schema_encoding)
        self.indirection.append(indirection)
        
class TailPage:
    def __init__(self, numCols):
        self.rid = []
        self.indirection = []
        self.pages = []
        self.schema_encoding = []
        self.num_records = 0

        for i in range(numCols):
            self.pages.append(Page())

    def has_capacity(self):
        if self.num_records < MAX_RECORDS_PER_PAGE:
            return True
        else:
            return False

    def insertRecTP(self, *columns, record):
        schema = '' #added schema encoding here because it's where I iterate through the data anyway
        for j in range(len(columns)):
            if columns[j] != None:
                self.pages[j].write(columns[j])
                schema = schema + '1'
            else:
                self.pages[j].write(record.columns[j])
                schema = schema + '0'

        self.schema_encoding.append(schema) #puts the schema encoding in
        self.num_records += 1

#Should have 16 BPs each BP with 4k Pages and each BP can have 4k records (1 record is a value from each page in BP)
#numCols gets sent to BasePage where it will determine number of Pages per Base Page
class PageRange:
    #Empty initialization
    def __init__(self, numCols):
        self.num_base_pages = 0
        self.num_tail_pages = 0
        self.basePages = []
        self.tailPages = []
        self.TPS = 0 # Or max?

        self.add_base_page(numCols) #every time page range created populate with one base page
    
    def create_page_range(self, cur_table_records):
        self.id = cur_table_records + 1

    # 16 base pages / page range
    def has_capacity(self):
        if self.num_base_pages < MAX_BASEPAGES_PER_RANGE:
            return True
        else:
            return False
    
    def mergePages(self):
        for tailPage in self.tailPages:
            for records in range(tailPage.num_records):
                rid = tailPage.rid[records]
                schema_encoding = tailPage.schema_encoding[records]

                for basePage in self.basePages:
                    if rid in basePage.rid:
                        basePageRecord = basePage.rid.index(rid)
                        for cols in range(len(schema_encoding)):
                            if schema_encoding[cols] == '1':
                                writeInValue = tailPage.pages[cols].get_value(records)
                                basePage.pages[cols].data[basePageRecord * 8 :(basePageRecord + 1) * 8] = int(writeInValue).to_bytes(8, byteorder = 'big')
            break

        self.tailPages = []
        self.num_tail_pages = 0

    def add_tail_page(self, numCols):
        self.tailPages.append(TailPage(numCols))
        self.num_tail_pages += 1
    
    def add_base_page(self, numCols):
        if self.has_capacity():
            self.basePages.append(BasePage(numCols))
            self.num_base_pages += 1
            #print("base page has been added")

            
      
