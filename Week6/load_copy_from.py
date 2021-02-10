
# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2.extras
import argparse
import re
import csv
import sys
import pandas as pd
import numpy as np

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
TableName = 'CensusDatacopyFrom'
Datafile = "Oregon2015.csv"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015
tmp_df = "/home/agrawal/tmp_dataframe.csv"

def initialize():
  global Year

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  #parser.add_argument("-c", "--createtable", action="store_true")
  parser.add_argument("-y", "--year", default=Year)
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  #global CreateDB
  #CreateDB = args.createtable
  Year = args.year

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	csvfile = open(fname, mode="r")
	csvfile.readline() #skip the header
	'''
        dr = csv.DictReader(fil)
	headerRow = next(dr)
	# print(f"Header: {headerRow}")
        rowlist = []
	for row in dr:
        rowlist.append(row)
        '''
	return csvfile


# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
        	DROP TABLE IF EXISTS {TableName};
 
        	CREATE UNLOGGED TABLE {TableName} (
		Year                INTEGER,
              CensusTract         NUMERIC,
            	State               TEXT,
            	County              TEXT,
            	TotalPop            INTEGER,
            	Men                 INTEGER,
            	Women               INTEGER,
            	Hispanic            DECIMAL,
            	White               DECIMAL,
            	Black               DECIMAL,
            	Native              DECIMAL,
            	Asian               DECIMAL,
            	Pacific             DECIMAL,
            	Citizen             DECIMAL,
            	Income              DECIMAL,
            	IncomeErr           DECIMAL,
            	IncomePerCap        DECIMAL,
            	IncomePerCapErr     DECIMAL,
            	Poverty             DECIMAL,
            	ChildPoverty        DECIMAL,
            	Professional        DECIMAL,
            	Service             DECIMAL,
            	Office              DECIMAL,
            	Construction        DECIMAL,
            	Production          DECIMAL,
            	Drive               DECIMAL,
            	Carpool             DECIMAL,
            	Transit             DECIMAL,
            	Walk                DECIMAL,
            	OtherTransp         DECIMAL,
            	WorkAtHome          DECIMAL,
            	MeanCommute         DECIMAL,
            	Employed            INTEGER,
            	PrivateWork         DECIMAL,
            	PublicWork          DECIMAL,
            	SelfEmployed        DECIMAL,
            	FamilyWork          DECIMAL,
            	Unemployment        DECIMAL
         	);	
         	ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract); 
    	""")

		print(f"Created {TableName}")

def load(conn, csvfile):

	with conn.cursor() as cursor:
		start = time.perf_counter()
		cursor.copy_from(csvfile, TableName, sep=",")
		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
	initialize()
	conn = dbconnect()
	df = pd.read_csv(Datafile)
	#print(df.shape[0])
	df = df.replace(r'^\s*$', np.nan , regex=True)
	df.fillna(0, inplace=True)
	df.insert(0, 'Year', Year)
	#print(df.shape[0])
	df["County"] =df["County"].replace('\'','')
	df.to_csv(tmp_df, index= False, header=False)
	#print(df.shape[0]) 
	csvfile = open(tmp_df, 'r')
	if CreateDB:
		createTable(conn)
	load(conn, csvfile)


if __name__ == "__main__":
    main()



