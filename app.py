import sys
from flask import Flask,render_template,request
import time   #query time-
import cStringIO
import pymysql
from flask import Flask
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from flask import render_template
from flask import request
import glob
import os
import datetime
import hashlib
import pickle as cPickle
import csv
import pylab
from werkzeug.utils import secure_filename

hostname = '********'
username = '********'
password = '********'
database = '********'

myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database, cursorclass=pymysql.cursors.DictCursor, local_infile=True)
print 'DB connected'

application = Flask(__name__)
app=application

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_ROOT = os.path.dirname(APP_ROOT)
print APP_ROOT

@app.route('/')
def main():
	return render_template("index.html")

@app.route("/file_up", methods = ['POST'])
def file_up():#For uploading the file
		file = request.files['filecsv']	
		filename=file.filename
		session['file_name']=filename
		filename = secure_filename(file.filename)
		file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
		cursor.execute ("DROP TABLE IF EXISTS employee")
		try:
			with Conn.cursor() as curs:
				curs.execute(droptablequery)
				Conn.commit()
		finally:
			Conn.close()
		linect="("
		absfilename=UPLOAD_FOLDER+filename
		infile = open(abs_filename,'r')
		for lines in infile:
			line=lines
			break
		for i in line:
			linect+=i+" VARCHAR(50),"
		qcreatetable="Create table if not exists " + filename[:-4]+linect+" sr_no INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(sr_no));"
		try:
			with Conn.cursor() as curs:
				curs.execute(qcreatetable)
				Conn.commit()
			curs.close()
		except curs.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))
		sys.exit(1)	
	
		inserttable="""LOAD DATA LOCAL INFILE '"""+absfilename+ 
		"""'INTO TABLE """+ filename[:-4] +
		""" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"""
		try:
			with Conn.cursor() as curs:
				curs.execute(inserttable)
				Conn.commit()
		finally:
			Conn.close()
		return render_template("filehandle.html")


@app.route("/uploadCsv", methods = ['POST'])
def csv_uploadCsv():
	global file_name
	f=request.files['upload_files']
	file_name=f.filename	
	print (file_name)
	titlename=request.form['file']
	global newfile
	newfile=os.path.abspath(file_name)
	print "new file down"
	print newfile
	block_csv.create_blob_from_path('shwetha',file_name,newfile,content_settings=ContentSettings(content_type='text/csv'))
	return "done"

@app.route("/load_db", methods = ['POST'])
def load_db():#For uploading the file
	#global file_name'
	UPLOAD_FOLDER="/home/shwethakonairysuresha/cdassign/"
	csv_file = request.files['file_upload']	
	file_name=csv_file.filename
	#session['file_name']=file_name
	print "file recieved"
	#filename = secure_filename(csv_file.filename)
	#csv_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
	drop_query="drop table IF EXISTS "+ file_name[:-4]
	with myConnection.cursor() as cursor:
		#self.cursor.execute(drop_query)
		myConnection.commit()
	print "dropped"
	column_name="("
	abs_filename=UPLOAD_FOLDER+file_name
	with open(abs_filename, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			line=row
			break
	for i in line:
		column_name+=i+" VARCHAR(50),"
	query="Create table if not exists " + file_name[:-4]+column_name+" sr_no INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(sr_no));"
	print query
	starttime = time.time()
	with myConnection.cursor() as cursor:
		cursor.execute(query)
		myConnection.commit()
	cursor.close()
	print "successfully created"
	#insert_str = r"LOAD DATA LOCAL INFILE + abs_filename + INTO TABLE + file_name[:-4]+  FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 lines"
   	#newline="\\\n"
	#new_char=newline[1:3]
	#print new_char
	insert_str="""LOAD DATA LOCAL INFILE '"""+abs_filename+ """' INTO TABLE """+ file_name[:-4] +""" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"""
	print (insert_str)
	with myConnection.cursor() as cursor:
		cursor.execute(insert_str)
		myConnection.commit()
	endtime = time.time()
	count_str="SELECT count(*) FROM "+ file_name[:-4]
	with myConnection.cursor() as cursor:
		cursor.execute(count_str)
		result=cursor.fetchall()
	print "successfully loaded"
	totalsqltime = endtime - starttime 
	return render_template("index.html")

@app.route("/instruct", methods = ['POST'])
def instruct():#For uploading the file
	username_be=request.form['inst']
	query="select * from classes where Instructor='"+username_be+"'"
	with myConnection.cursor() as cursor:
		cursor.execute(query)
		result=cursor.fetchall()
	return render_template("filehandle.html",result=result)

@app.route("/showlist", methods=['POST'])
def showlist():
	name=request.form['coursename']
	sid=request.form['sid']
	query="insert into enroll values('"+course_name+"','"+studentid+"')"
	with myConnection.cursor() as cursor:
		cursor.execute(query)
	return render_template("filehandle.html")

if __name__ == "__main__":
	app.run(debug=True,host='0.0.0.0')


