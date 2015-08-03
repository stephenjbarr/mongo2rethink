#!/usr/bin/env python3
# coding: utf-8


## Initial imports

import pymongo
import time
import rethinkdb as r
import sjbsettings
import inspect
import datetime
import bson
from pymongo import MongoClient
client = MongoClient(host=sjbsettings.sjb['MHOST'])
DBNAME = 'demomodel'
db = client[DBNAME]
REMOVE_COLS = set(['_properties', 'system.indexes'])


## Connect to RethinkDB # TODO Make Rethink and Mongo hosts in different variables
rt_conn = r.connect(db = 'demomodel', host=sjbsettings.sjb['MHOST']).repl()



################################################################################
## For a Mongo database, get the collections in this database
## and subtract REMOVE_COLS. The result of calling this function
## are the columns that you will mirror to RethinkDB
def getMongoCollections(db):
    colset = set(db.collection_names())
    return(set.difference(colset, REMOVE_COLS))

## For a Mongo document, transform it to an object that can be serialized into
## JSON for insertion into RethinkDB. If there are ever serialization issues,
## the fix gets implemented here.
def transform_mongo_doc_to_rethink_doc(mdoc):
    for k,v in mdoc.items():

        print("k: " + str(k) + " type: " + str(type(v)))

        ## fix subdocuments
        if(type(v) == dict):
            mdoc[k] = transform_mongo_doc_to_rethink_doc(v)
        
        ## fix timezone, set to UTC
        ## RethinkDB will not accept dates without a timezone.
        ## # TODO fix to check for an existing timezone before overwriting with UTC
        if(type(v) == datetime.datetime):
            mdoc[k] = v.replace(tzinfo=r.make_timezone('0:00'))
    
        ## fix object id
        if k == "_id":
            mdoc[k] = str(v)

        if(type(v) == bson.objectid.ObjectId):
            mdoc[k] = str(v)

        if(type(v) == list):
            mdoc[k] = [str(x) for x in v]

        # ## fix object id
        # if k == "_id":
        #     mdoc[k] = str(v)

        # if k == "_etag":
        #     mdoc[k] = str(v)


            
    return(mdoc)
################################################################################



rt_collections = r.db(DBNAME).table_list().run(rt_conn)

collections_in_mongo_not_rt = set.difference(getMongoCollections(db), set(rt_collections))

for missing_collection in collections_in_mongo_not_rt:
    print("Creating " + missing_collection + " which was missing in rethink")
    r.db(DBNAME).table_create(missing_collection).run(rt_conn)




## the main loop

## # TODO, make rethink_mirrored a settable column name
query = { "$or" :  [ {"rethink_mirrored" : {"$exists" : False}}, { "rethink_mirrored" : False}]}


while 1:

    for collection in cols:

        cur_col = db[collection]
        N_docs  = cur_col.count(query)
            
        if N_docs > 0:
            print("--------------------------------------------------------------------------------")
            print(collection + "\t\t" + str(N_docs) + " unmirrored docs")
            for doc in cur_col.find(query):
                print("DOC:")
                print(str(doc))
                rt_doc = transform_mongo_doc_to_rethink_doc(doc)
                rt_doc['rethink_mirrored'] = True ## this becomes true on the next line
                print(str(rt_doc))
                r.table(collection).insert(rt_doc).run(rt_conn)


            ud_res = cur_col.update_many(query, {"$set" : {"rethink_mirrored" : True}}, upsert = False)
            print("\tUpdate success?" + str(ud_res.acknowledged))

    time.sleep(1)


