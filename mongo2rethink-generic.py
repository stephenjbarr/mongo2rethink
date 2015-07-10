#!/usr/bin/env python3
# coding: utf-8

# In[1]:

import pymongo
import time
import rethinkdb as r
import sjbsettings
import inspect
import datetime
from pymongo import MongoClient
client = MongoClient(host=sjbsettings.sjb['MHOST'])
DBNAME = 'demomodel'
db = client[DBNAME]
REMOVE_COLS = set(['_properties', 'system.indexes'])


# In[2]:

rt_conn = r.connect(db = 'demomodel', host=sjbsettings.sjb['MHOST']).repl()


# In[3]:

db.collection_names()


# In[4]:

def getMongoCollections(db):
    colset = set(db.collection_names())
    return(set.difference(colset, REMOVE_COLS))


# In[5]:


def transform_mongo_doc_to_rethink_doc(mdoc):
    for k,v in mdoc.items():
        ## fix subdocuments
        if(type(v) == dict):
            mdoc[k] = transform_mongo_doc_to_rethink_doc(v)
        
        ## fix timezone
        if(type(v) == datetime.datetime):
            mdoc[k] = v.replace(tzinfo=r.make_timezone('0:00'))
    
        ## fix object id
        if k == "_id":
            mdoc[k] = str(v)

        if k == "_etag":
            mdoc[k] = str(v)

    return(mdoc)


# In[6]:

getMongoCollections(db)


# In[7]:

rt_collections = r.db(DBNAME).table_list().run(rt_conn)

collections_in_mongo_not_rt = set.difference(getMongoCollections(db), set(rt_collections))
collections_in_mongo_not_rt

for missing_collection in collections_in_mongo_not_rt:
    print("Creating " + missing_collection + " which was missing in rethink")
    r.db(DBNAME).table_create(missing_collection).run(rt_conn)


# In[9]:

## the main loop

cols = list(getMongoCollections(db))
##cols = ["rawdata_sensor"]
query = { "$or" :  [ {"rethink_mirrored" : {"$exists" : False}}, { "rethink_mirrored" : False}]}


while 1:

    for collection in cols:

        cur_col = db[collection]
        N_docs  = cur_col.count(query)
            
        if N_docs > 0:
            print("--------------------------------------------------------------------------------")
            print(collection + "\t\t" + str(N_docs) + " unmirrored docs")
            for doc in cur_col.find(query):
                rt_doc = transform_mongo_doc_to_rethink_doc(doc)
                rt_doc['rethink_mirrored'] = True ## this becomes true on the next line
                print(str(rt_doc))
                r.table(collection).insert(rt_doc).run(rt_conn)


            ud_res = cur_col.update_many(query, {"$set" : {"rethink_mirrored" : True}}, upsert = False)
            print("\tUpdate success?" + str(ud_res.acknowledged))

    time.sleep(1)


