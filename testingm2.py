from lstore.db import Database
# from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open("./ECS165")
db.create_table("grades", 5, 0)
db.close()