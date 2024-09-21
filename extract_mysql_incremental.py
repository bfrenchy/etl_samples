import pymysql
import csv
import boto3
import configparser
import psycopg2
import os

"""
This script pulls data from an Amazon Redshift Cluster (warehouse)
And finds the latest change,
extracts this from MySQL, and pushes to the S3 bucket.
"""

# Get Redshift connection info
parser = configparser.ConfigParser()
parser.read("pipeline.conf")

dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

# Connect to redshift cluster
rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port)

rs_sql = """
    SELECT COALESCE(MAX(LastUpdated),'1900-01-01')
    FROM awstutorial.Orders;
"""
rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone()

# Now we got the latest updated row
last_updated_warehouse = result[0]

rs_cursor.close()
rs_conn.commit()

# Initialize MySQL connection - hosted through AWS RDS (Rel. DB System)
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(
    host=hostname,
    user=username,
    password=password,
    db=dbname,
    port=int(port)
)

if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established!")

"""
Run a partial extraction of the Orders table, and do the following:
- Extract entire table contents of the table
- Write the results to a pipe-delimited CSV file.
"""

m_query = "SELECT * FROM awstutorial.Orders WHERE LastUpdated > %s;"
local_filename = "orders_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query, (last_updated_warehouse))  # Only extract new values
results = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)
    fp.close()

m_cursor.close()
conn.close()

"""
Now that we have the local file (a temporary file), upload to an AWS S3 bucket.
This is a storage location for later loading into a:
- data warehouse
- somewhere else.
"""

# Load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

s3_file = local_filename
s3.upload_file(local_filename, bucket_name, s3_file)

# delete the local file after upload is done
os.remove(local_filename)
