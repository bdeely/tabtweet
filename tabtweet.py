#To-Do List
#Make a few error handling functions
#make it append data so that it can be run at different times without just overwriting the original file

from TwitterAPI import TwitterAPI
import time
import dataextract as tde
import os

#All of this is for creating the Tableau extract
tdename = 'tableautwitter.tde'
try:
    tdefile = tde.Extract(tdename)
except:
    os.remove(tdename)
    tdefile = tde.Extract(tdename)

fields = ['created_at', 'id', 'text', 'source', 'lang', 'place']
    
#Create the tableDef
tableDef = tde.TableDefinition()
tableDef.addColumn('user', tde.Type.UNICODE_STRING)
tableDef.addColumn('created_at', tde.Type.UNICODE_STRING)
tableDef.addColumn('id', tde.Type.CHAR_STRING)
tableDef.addColumn('text', tde.Type.UNICODE_STRING)
tableDef.addColumn('lang', tde.Type.UNICODE_STRING)
tableDef.addColumn('country_name', tde.Type.UNICODE_STRING)
tableDef.addColumn('longitude', tde.Type.DOUBLE)
tableDef.addColumn('latitude', tde.Type.DOUBLE)
tableDef.addColumn('country_code', tde.Type.UNICODE_STRING)
tableDef.addColumn('place_type', tde.Type.UNICODE_STRING)
tableDef.addColumn('full_name', tde.Type.UNICODE_STRING)

#Step 3: Create the table in the image of tableDef
table = tdefile.addTable('Extract', tableDef)




#Right below we authenticate ourselves. Enter your own stuff; not sharing mine!
consumerkey = ''
consumersecret = ''
authtoken = ''
authsecret = ''

api = TwitterAPI(consumerkey,consumersecret,authtoken,authsecret)

r = api.request('statuses/sample', {})

item_count = 0
place_count = 0
coord_count = 0

#Set the number of Tweets you want; here 10,000
TWEETS_TO_GET = 10000

#time here is just for debugging/testing as the code runs; timeouts and such
t = time.time()

#here we actually loop through Twitter's JSON metadata to pull it into our TDE columnar format
for item in r.get_iterator():
    try:
        newrow = tde.Row(tableDef)
        newrow.setString(0,item['user']['screen_name'])
        newrow.setString(1,item['created_at'])
        newrow.setCharString(2,str(item['id']))
        newrow.setString(3,item['text'])
        newrow.setString(4,item['lang'])
        #newrow.setCharString(5,item['place'])
        
        if item['place']: 
            #print item['place']
            #we have ['place']['ABC'] because place is kind of like a sub-hierarchy within the JSON
            newrow.setString(5,item['place']['country'])
            newrow.setString(8,item['place']['country_code'])
            newrow.setString(9,item['place']['place_type'])
            newrow.setString(10,item['place']['full_name'])
            place_count += 1
        
        if item['coordinates']:
            newrow.setDouble(6,item['coordinates']['coordinates'][0])
            newrow.setDouble(7,item['coordinates']['coordinates'][1])
            coord_count += 1
        
        #insert the data we just cleaned up
        table.insert(newrow)
        item_count += 1
        print item_count
        if not i % 10: print i
        if item_count == TWEETS_TO_GET: 
            tdefile.close()
            break
        print item
        
        
    except:
        continue

print time.time() - t, p, c

tdefile.close()
