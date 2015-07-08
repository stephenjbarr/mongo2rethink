#!/usr/bin/env python3

import rethinkdb as r
import sjbsettings

conn = r.connect(db = 'demomodel', host=sjbsettings.sjb['MHOST']).repl()

for change in r.table('prediction').changes().run(conn):
    print(str(change))
