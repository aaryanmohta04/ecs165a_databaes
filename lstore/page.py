# UPDATE IN TESTING
#
# Following example in milestone1:
# PAGE_SIZE = 4096 bytes
# Page Range has 64k records
# Base Page/ tail page has 1 page per column
# Page Range has 16 base pages + any tail pages
# Number of records per base page = 4000
# How many records per page?
# 4000 or 4000/ num_base_page_columns ???
# Is this even hard coded?
RECORDS_PER_PAGE =

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records < RECORDS_PER_PAGE
            return true
        else
            return false
        pass

    def write(self, value):
        self.num_records += 1
        pass

class PageRange:
    def _init_(self):
        # 16 base pages / page range
        self.base_pages = [None] * 16
        self.tail_pages = [None]