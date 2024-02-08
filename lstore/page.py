# UPDATE IN TESTING
#
# Following example in milestone1:
# PAGE_SIZE = 4096 bytes
# Page Range has 64k records
# Base Page/ tail page has 1 page per column
# Page Range has 16 base pages + any tail pages
# Number of records per base page = 4000


# How many records per page?
    #also 4k i believe because each page is just a column for base pages (which is 4k)
# 4000 or 4000/ num_base_page_columns ???
    #one record is a value for each column/page, so just 4k
# Is this even hard coded?
    #i think so

#for arrays and stuff
import numpy as np

MAX_RECORDS_PER_PAGE = 4000
MAX_PAGES_PER_RANGE = 16
MAX_RECORDS_PER_PAGE_RANGE = MAX_RECORDS_PER_PAGE * MAX_PAGES_PER_RANGE

#One Page for Every Column in Table (maybe 4k pages/columns per base page)
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records < MAX_RECORDS_PER_PAGE:
            return true
        else:
            return false

    def write(self, value):
        self.num_records += 1
        self.data.append(value)

#One Base_Page Contains many pages/columns (16 BPs in Page Range)
#Technically Tail Pages also create 4k columnar pages (could this class be used for both?)
class BasePage:

    #need to know how many records in BP (4k is max)
    #need 4k pages/columns per BP
    #need to create an array of pages?
    #creating 4k columnar pages for each BP
    def __init__(self):
        self.num_records_BP = 0
        for i in range(4000):
            ColumnPageArray[i] = Page()

    def has_capacity(self):
        if self.numREcords_BP < MAX_RECORDS_PER_PAGE:
            return true
        else:
            return false

#Should have 16 BPs each BP with 4k Pages and each BP can have 4k records (1 record is a value from each page in BP)
class PageRange:
    def __init__(self):
        # 16 base pages / page range
        self.num_base_pages = 0
        self.num_tail_pages = 0
        #maybe the RID of the first record is the page range id? if we even need one
        self.id = 0
        self.base_pages = [None] * 16
        self.tail_pages = [None]
        
    def create_page_range(self, cur_table_records):
        self.id = cur_table_records + 1

    def has_capacity(self):
        if self.num_base_pages < MAX_PAGES_PER_RANGE:
            return true
        else:
            return false

    def add_page(self, page):
        self.num_base_page += 1
        pass
