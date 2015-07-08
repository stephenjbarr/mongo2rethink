#!/usr/bin/env python3
# coding: utf-8

# In[5]:

import pymongo
import time


# In[6]:

import rethinkdb as r
import sjbsettings


# In[10]:

from pymongo import MongoClient
client = MongoClient(host=sjbsettings.sjb['MHOST'])


# In[11]:

db = client['demomodel']
preds = db['prediction']


# In[13]:

conn = r.connect(db = 'demomodel', host=sjbsettings.sjb['MHOST']).repl()
for pred in r.table('prediction').run(conn):
    print(pred)


# In[47]:

seen_keys = {}


# In[68]:

insert_into_rethink = True

while 1:
    for pred in preds.find():
        pid = pred['_id']
        if pid not in list(seen_keys.keys()):
            # print(str(pred) + " is new!")
            # print(str(r.iso8601(pred['update_t'])))
            ## pred['update_t'] = r.make_timezone(pred['update_t'])
            pred['_id'] = str(pred['_id'])
            pred['update_t'] = pred['update_t'].replace(tzinfo=r.make_timezone('0:00'))
            print("Updating " + pred['uid'] + " at " + str(pred['update_t']))
            if insert_into_rethink:
                seen_keys[pid] = True
                r.table('prediction').insert(pred).run(conn)

    time.sleep(0.1)



