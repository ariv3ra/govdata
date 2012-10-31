import os
import python_usdol
import json
import time
import datetime
from pymongo import Connection

tkn = None   #Defines your token
sct = None   #Defines your secret
skipval = 0  #Defines the number of records to skip
topval = 100 #Defines the max amount of records to retrieve
wFlag = False #Defines the flag for the test inteh while loop
num = 0
DATASET = 'Safety/Fatalities'
TABLE = 'OSHA_Fatalities'

#Build the variables that use the assigned environment variables
HOST = os.environ['OPENSHIFT_NOSQL_DB_HOST']
PORT = int(os.environ['OPENSHIFT_NOSQL_DB_PORT'])
DB_USER = os.environ['OPENSHIFT_NOSQL_DB_USERNAME']
DB_PWD = os.environ['OPENSHIFT_NOSQL_DB_PASSWORD']
DB_NAME = 'govdata' #data base name

def createMongoURI(host, port, user, pwd, dbname):
    s = "mongodb://" + user + ":" + pwd + "@" + host + ":" + port +"/" + dbname
    return s

dictFileCont = []  #Defines the content of the JSON data string
dictMissingState = []

#Setup the database connections
mconn = Connection(createMongoURI(HOST, str(PORT), DB_USER, DB_PWD, DB_NAME))
db = mconn.dol

#get DOL API tokens and secrets
fl = open("config")  #Open the config file
for f in fl:
    ln = f.split("=")
    if ln[0] == "token":
        tkn = ln[1].strip()  #Assign the token value
    else:
        sct = ln[1].strip()  #Assign the secret value

conn = python_usdol.Connection(tkn,sct)  #create an instance of the API connection

def isData(ds,tb,topvalue, skipvalue):
#Retrive data
    row = conn.fetch_data(ds, tb, top=str(topvalue), skip=str(skipvalue), select='id')
    #Check if there is data
    if row:
        return True
    else:
        return False

# Pull data and check if there are elements to parse
while isData(DATASET,TABLE,topval,skipval):
    data = conn.fetch_data(DATASET, TABLE, top=str(topval), skip=str(skipval), orderby='id asc')
    #data = conn.fetch_data('Safety/Fatalities', 'OSHA_Fatalities', orderby = 'id desc', top = 1, skip = 30)
    #data = conn.fetch_data('Safety/Fatalities','OSHA_Fatalities')
    #data = conn.fetch_data('Compliance/OSHA','full')

    for d in data:
        
        c = d['dateofincident']
        start = c.find("(")+1
        end = c.find(")")
        
        ep = c[start:end]
        
        t = datetime.datetime.utcfromtimestamp(float(ep)/1000.)
        #fmt = "%Y-%m-%d %H:%M:%S"
        #fmt = "%Y-%m-%d"
        
        fmt = "%Y,%m,%d"
        print "Time: " + str(t)
        #d['dateofincident'] = t.strftime(fmt) # Update the date value
        
        d['dateofincident'] = t # Update the date value

        ###
        #  Here we are going to parse and separate the values of the companyandlocation field
        ###
        comp = d['companyandlocation'].strip().split(',')
        
        ## Separate the state and zip fields           
        d['company'] = comp[0]  
        
        element = len(comp)-1  #Get the total number of elements minus 1 so it gets the last element
        ch = comp[element].split()  #assign the state and zip values to ch as array
        
        print "The number of elements in company: " + str(len(comp)) 
        
        #print "check the failed item and it is = " + str(comp[element].split())
        if ch:
            print "####   The element is valid - length is:" + str(len(ch))
            d["state"] = ch[0]
            if len(ch) > 1:
                d["zip"] = ch[1]
        else:
            print '####   The data is valid don\'t proceed'
            dictMissingState.append(d)
            d["state"] = "unknown"
            
        num += 1
        ##Pop these fields from the row
        d.pop('__metadata')
        d.pop('companyandlocation')
        # Build the JSON data set       
        dictFileCont.append(d)
        
        print "Record: " + str(num) 
        print d
        
    skipval += 100  # increment the skip value by 100
'''
#Dump the data to a file
with open("oshafatals.txt","wb") as f:
    json.dump(dictFileCont, f)
    
#Dump the records with incomplete or dirty company and state data    
with open("oshafatalsMissingData.txt","wb") as f:
    json.dump(dictMissingState, f)
'''
#Dump this to mongoDB
db.fatals.insert(dictFileCont)
