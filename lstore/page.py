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
            return true
        else:
            return false

    def write(self, value):
        # 8 bytes per record
        self.data[num_records * 8] = value
        self.num_records += 1

#One Base_Page Contains many pages/columns (16 BPs in Page Range)
#Technically Tail Pages also create 4k columnar pages (could this class be used for both?)
class BasePage:

    #need to know how many records in BP (4k is max)
    #need 4k pages/columns per BP
    #need to create an array of pages?
    #creating 4k columnar pages for each BP
    def __init__(self, numCols):
        self.rid = [] 
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.pages = []
        self.num_records = 0

        #create array of pages/cols (4 allocated for schema, indirection, rid, start time)
        for i in numCols:
            self.pages.append(Page())

    def has_capacity(self):
        if self.num_records < MAX_RECORDS_PER_PAGE:
            return true
        else:
            return false
            
    def insertRecBP(self, *columns):
        for i in numCols: #iterates through number of columns and writes data in *columns to corresponding page in page[] 
            self.pages[i].write(columns[i])
        self.num_records += 1
        
class TailPage:
    def __init__(self):
        self.rid = []
        self.indirection = []
        self.pages = []

#Should have 16 BPs each BP with 4k Pages and each BP can have 4k records (1 record is a value from each page in BP)
#numCols gets sent to BasePage where it will determine number of Pages per Base Page
class PageRange:
    #Empty initialization
    def __init__(self, numCols):
        self.num_base_pages = 0
        self.num_tail_pages = 0
        self.basePages = []
        self.tailPages = []

        self.base_pages.append(add_base_page(numCols)) #every time page range created populate with one base page
    
    def create_page_range(self, cur_table_records):
        self.id = cur_table_records + 1

    # 16 base pages / page range
    def has_capacity(self):
        if self.num_base_pages < MAX_BASEPAGES_PER_RANGE:
            return true
        else:
            return false
            
    def add_tail_page(self):
        pass
    
    def add_base_page(self, numCols):
        if self.has_capacity():
            self.base_pages.append(BasePage(numCols))
            self.num_base_pages += 1
        pass
