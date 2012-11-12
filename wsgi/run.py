import os
import datetime
from flask import Flask
from flask import render_template
from flask import flash, redirect, url_for, request, make_response
from flask.ext.pymongo import PyMongo

#Create an instance of flask
app = Flask(__name__)

#Build the variables that use the assigned environment variables
HOST = os.environ['OPENSHIFT_NOSQL_DB_HOST']
PORT = int(os.environ['OPENSHIFT_NOSQL_DB_PORT'])
DB_USER = os.environ['OPENSHIFT_NOSQL_DB_USERNAME']
DB_PWD = os.environ['OPENSHIFT_NOSQL_DB_PASSWORD']
DB_NAME = 'govdata' #data base name

app.config['MDB_HOST'] = HOST
app.config['MDB_PORT'] = PORT
app.config['MDB_USERNAME'] = DB_USER
app.config['MDB_PASSWORD'] = DB_PWD
app.config['MDB_DBNAME'] = DB_NAME

mdb = PyMongo(app, config_prefix='MDB') #Create instance of PyMongo object

@app.template_filter("frmdate")
def frmDate(s):
    d = s.strftime("%m/%d/%Y")
    return d

@app.route("/")
@app.route("/index")
def index():
    title = "fatalQuery"  
    return render_template("index.html", title = title)

@app.route("/view", methods=["GET","POST"])
def view():
    year = request.form['year']
    state = request.form['state']
    result = mdb.db.fatals.find({"$and":[{"year":int(year)},{"state":state}]}).sort('dateofincident',1)
    count = mdb.db.fatals.find({"$and":[{"year":int(year)},{"state":state}]}).count()
    return render_template("view.html", year = year, state = state, result = result, count = count)

@app.route("/results", methods=["GET","POST"])
def results():
    year = request.form['year']
    state = request.form['state']
    
    results = mdb.db.fatals.find({"$and":[{"year":int(year)},{"state":state}]}).sort('dateofincident',1)
    return render_template("results.html", results = results)
    
if __name__ == "__main__":
    app.run(debug = "True")
    
