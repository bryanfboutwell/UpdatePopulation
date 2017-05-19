#!/usr/bin/python
import psycopg2
import pymongo
import pprint
from pymongo import MongoClient

#import file to mongodb (run one time)
#mongoimport --db zips --collection zipcodes --drop --type json --file "C:/Users/bryanb.PDC/Documents/ETL Project/zips.json"

def update():
#Connect to Mongo and recalculate population
   client = MongoClient('localhost', 27017)
   db = client.zips
   collection = db.zipcodes
   map = 'function() { emit(this.state, { sum: this.pop}); }'
   reduce = '''function(key, values) {
       var result = { sum: 0 };
       values.forEach(function(value) {
           result.sum += value.sum; })
       return result; }'''
   finalize = '''function(k, v) { 
       return { sum: v.sum } }
       '''
   collection.map_reduce(map, reduce, out="myResults",finalize=finalize)
   agg = db.myResults
   #print map reduce results
   for doc in agg.find():
      us_state = doc['_id']
      population = int(doc['value']['sum'])
      print("call sp with state: " + us_state + " and population: " + str(population))
      #connect to postgresql and update population
      #def UpdatePopulation(State,Pop):
      #conn = None
      #try:
      # connect to the PostgreSQL database
      conn = psycopg2.connect("host='localhost' dbname='States' user='postgres' password='Betsy526!'")
      # create a cursor object for execution
      cur = conn.cursor()
      # another way to call a stored procedure
      #cur.callproc('public.InsertUpdateStatePopulation',(State,Pop))
      cur.execute("SELECT public.InsertUpdateStatePopulation( %s, %s); ",(us_state,population))
      conn.commit()
      # process the result set
      row = cur.fetchone()
      #while row is not None:
      #    print(row)
      #    row = cur.fetchone()
      print(row)
      # close the communication with the PostgreSQL database server
      cur.close()
      conn.close()
      #except (Exception, psycopg2.DatabaseError) as error:
      #    print(error)
      #finally:
      #    if conn is not None:
      #        conn.close()
   print("Update complete")
   
