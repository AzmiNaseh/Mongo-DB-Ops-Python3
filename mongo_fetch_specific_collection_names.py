from pymongo import MongoClient


client = MongoClient("something.domain.com", 27017)

db = client.mydb
fId = []


def f_id():
	for i in db.list_collection_names():
		if "_somestring" in i:
			print (i)
			fId.append(i.split("_")[0])
	print(fId)



f_id()
