from lstore.db import Database
from lstore.query import Query


db = Database()
grades_table = db.create_table('Grades', 5, 0)
grades_table = db.create_table('Names', 5, 0)
grades_table = db.create_table('Years', 5, 0)
grades_table = db.drop_table('Names')
grades_table = db.create_table('Ages', 5, 0)