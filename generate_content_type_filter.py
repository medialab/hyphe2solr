

# mongodb
import pymongo,sys,json



try:
    with open('config.json') as confile:
        conf = json.loads(confile.read())
except Exception as e:
    print type(e), e
    sys.stderr.write('ERROR: Could not read configuration\n')
    sys.exit(1)

try:
    db = pymongo.Connection(conf['mongo']['host'], conf['mongo']['port'])[conf["mongo"]["db"]]
    coll = db[conf['mongo']['web_pages_collection']]
    index=coll.create_index([('content_type', pymongo.ASCENDING)], background=True)
except Exception as e:
    print type(e), e
    sys.stderr.write('ERROR: Could not initiate connection to MongoDB\n')
    sys.exit(1)

content_types_grouped = coll.aggregate( [ 
    { "$group": { "_id": "$content_type","count": {"$sum": 1}}  }
])
coll.drop_index(index)

with open("content_type_whitelist.csv","w") as content_type_whitelist :
	content_type_whitelist.write("%s,%s\n"%("content_type","count"))
	content_types_grouped["result"].sort(key=lambda e:e["count"],reverse=True)
	for d in content_types_grouped["result"] :
		content_type_whitelist.write("%s,%s\n"%(d["_id"],d["count"]))

#     with open(conf['mongo']['contenttype_whitelist_filename']) as content_type_whitelist :
#         accepted_content_types=content_type_whitelist.read().split("\n")
# with open("content_types.txt","w") as content_types_f:
# 	for content_type  in db[config['mongo-scrapy']['pageStoreCol']].distinct("content_type"):
#         content_types_f.write(content_type+"\n")


