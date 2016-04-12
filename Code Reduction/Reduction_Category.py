
from datetime import datetime
import os
path = os.getcwd()
loc_offers = path+'/offers.csv'
loc_transactions = path+'/transactions.csv'
loc_reduced = path+'/reduced_category' # will be created

def reduce_data(loc_offers, loc_transactions, loc_reduced):

  start = datetime.now()
  #get all categories on offer in a dict
  offers = {}
  for e, line in enumerate( open(loc_offers) ):
    offers[ line.split(",")[1] ] = 1
  #open output file
  with open(loc_reduced, "wb") as outfile:
    #go through transactions file and reduce
    reduced = 0
    for e, line in enumerate( open(loc_transactions) ):
      if e == 0:
        outfile.write( line ) #print header
      else:
        #only write when Company is in offers dict
          if line.split(",")[3] in offers:
            outfile.write( line )
            reduced += 1
      #progress
      if e % 5000000 == 0:
        print e, reduced, datetime.now() - start
  print e, reduced, datetime.now() - start

reduce_data(loc_offers, loc_transactions, loc_reduced)
