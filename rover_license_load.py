#!/usr/bin/python

import common_library
import json
import os
import urllib
import urllib2
import mysql.connector
import traceback
import copy
from datetime import datetime
import csv

conn = None

def getConn():
        """getConn
                This will set up the database connection.

        Args:

        Returns:
		conn: database connection

        """
	global conn
	if conn == None:
		#This is a wrapper around either Vault or Device42. Replace with your credential store method
		app_user,app_password = common_library.lookupPassword("YOUR app user")
		destination = {
			'user': app_user,
			'password': app_password,
			'host':'YOUR_SERVER',
			'database':'license'
		}
		conn = mysql.connector.connect(**destination)
	return conn

def initDB():
        """initDB
                This will Set up the database and table.  This is my poor mans version of Flyway

        Args:

        Returns:

        """

	#This is a wrapper around either Vault or Device42. Replace with your credential store method
	super_user,super_password = common_library.lookupPassword("YOUR DDL user")
	destination = {
		'user': super_user,
		'password': super_password,
		'host':'YOUR_SERVER'
	}
	conn = mysql.connector.connect(**destination)
	cursor = conn.cursor(dictionary=True,buffered=True)
	sql = """CREATE DATABASE IF NOT EXISTS license DEFAULT CHARACTER SET UTF8"""
	cursor.execute(sql)

	cursor.execute("USE license")

	sql = """CREATE TABLE IF NOT EXISTS seattle_license (
  		   `id` int(11) NOT NULL AUTO_INCREMENT,
  		   `license_number` varchar(30) NOT NULL,
  		   `license_issue_date` datetime NOT NULL,
  		   `animal_s_name` varchar(100) DEFAULT NULL,
  		   `species` varchar(50) DEFAULT NULL,
  		   `primary_breed` varchar(50) DEFAULT NULL,
  		   `secondary_breed` varchar(50) DEFAULT NULL,
  		   `zip_code` varchar(10) DEFAULT NULL,
  		   `update_dt` datetime NOT NULL,
  		   PRIMARY KEY (`id`)
		   ) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 """
	cursor.execute(sql)

	#Grant the app user CRUD access
	app_user,app_password = common_library.lookupPassword("YOUR App user")
	sql = "GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE on license.* to %s"
	cursor.execute(sql,[app_user])
	cursor.close()


def initCache():
        """initCache
                This will query the database to get the current dataset.

        Args:

        Returns:
		rowset: array of dict

        """
	conn = getConn()
	cursor = conn.cursor(dictionary=True,buffered=True)

	sql = """SELECT 
                   * 
		 FROM
		   seattle_license"""

	cursor.execute(sql)

	rowset={}
	for row in cursor:
		rowset[row['license_number']] = copy.copy(row)

		
	cursor.close()
	return rowset
                   
if __name__ == "__main__":
        """Main Function
                This will open the data file or call the API if none is provided
		It then caches the data set from the mysql table, and compares the rows in the new dataset to the database.
		
		The natural key for this data is license_number
		Any new license_numbers will be inserted.
		Any matched license_numbers will be compared for change, and it the row has changed, they will be updated.

        """
	# These are the fields for the dataset
	keylist = ["license_issue_date","license_number","animal_s_name","species","primary_breed","secondary_breed","zip_code"]

	# this is an initial non existant data set
	parsed_json = None

	if os.path.exists("jguv-t9rb.json"):
		#Read JSON from file
		fp = open("jguv-t9rb.json","r")
		text = fp.read()
		fp.close
		parsed_json = json.loads(text)
	elif os.path.exists("jguv-t9rb.csv"):
		#Read CSV file and create a JSON
		fp = open("jguv-t9rb.csv","r")
		text = fp.read()
		parsed_json = []
		for line in  csv.reader(text.replace("\r","").split("\n"), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
			if len(line) == 0 or "License Issue Date" == line[0]:
				continue
			lineDict = dict(zip(keylist,line))
			if lineDict['license_issue_date'] != "":
				lineDict['license_issue_date'] = datetime.strptime(lineDict['license_issue_date'], '%B %d %Y')
			parsed_json.append(lineDict)
			
	else:
		#Read JSON fromREST API
		url = "https://data.seattle.gov/resource/jguv-t9rb.json"
		headers = {"X-App-Token":"YOURTOKENHERE"}
		req = urllib2.Request(url, headers=headers)
        	response = urllib2.urlopen(req)
        	text = response.read()
		parsed_json = json.loads(text)

	#Initialize the DB, because I don't have flyway
	initDB()

	#Initialize the data cache from the database
	#The key for the cache is license_number
	cache = initCache()
	cursor = conn.cursor(dictionary=True,buffered=True)
	cursor.execute("USE license")

	#Initialize dictionary of counts
	report= {"inserted":0,"updated":0,"total":0}

	#Compare each record in the source dataset to the database dataset
	for record in parsed_json:
		report['total'] += 1

		#Make sure all the fields required for the SQL dataset are present
		for key in keylist:
			if key not in record.keys():
				record[key] = ""

		if record['license_number'] not in cache.keys():
			#insert
			report['inserted'] += 1
			sql="""INSERT INTO 
                                 seattle_license
			         (
                  	           `license_number`,
                   	           `license_issue_date`,
                   	           `animal_s_name`,
                   	           `species`,
                   	           `primary_breed`,
                   	           `secondary_breed`,
                   	           `zip_code`,
                   	           `update_dt`
                                 )
			       VALUES
			         (
                  	           %(license_number)s,
                   	           %(license_issue_date)s,
                   	           %(animal_s_name)s,
                   	           %(species)s,
                   	           %(primary_breed)s,
                   	           %(secondary_breed)s,
                   	           %(zip_code)s,
                   	           now()
                                 )"""

			try:
				cursor.execute(sql, record)
			except:
				print "Error on "+record['license_number']
				print record
				print cursor.statement
				print traceback.format_exc()

		else:
			#Copy the id for the row from the cache to the new dataset
			record['id'] = cache[record['license_number']]['id']
			for keys in cache[record['license_number']].keys():
				#if any of the fields are different, update and stop looking for differences.
				if record[key] != cache[record['license_number']][key]:
					#update
					report['updated'] += 1
					sql="""UPDATE
                                 		 seattle_license
					       SET
                  	           		 `license_number` = %(license_number)s,
                   	           		 `license_issue_date` = %(license_issue_date)s,
                   	           		 `animal_s_name` = %(animal_s_name)s,
                   	           		 `species` = %(species)s,
                   	           		 `primary_breed` = %(primary_breed)s,
                   	           		 `secondary_breed` = %(secondary_breed)s,
                   	           		 `zip_code` = %(zip_code)s,
                   	           		 `update_dt` = now()
					       WHERE
						 `id` = %(id)s"""

					try:
						cursor.execute(sql, record)
					except:
						print "Error on "+record['license_number']
						print record
						print cursor.statement

					break

	#Commit.  In a larger dataset we would commit in batches
	conn.commit()

	#Clean up
	cursor.close()
	conn.close()

	#Let people know how we did. 
	print report
		
