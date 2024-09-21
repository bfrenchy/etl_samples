import boto3
import configparser
import psycopg2

"""
Loads data from the S3 bucket to a Redshift data warehouse.

To do so, first create the destination table in the warehouse:
CREATE TABLE public.Orders(
    OrderId int,
    OrderStatus varchar(30),
    LastUpdated timestamp
);

Follow the steps in quotes for a full load

For an incremental load, no change is needed, 
this will preserve all records and changes, but not deletions
"""

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

# Connect to Redshift Cluster
rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port
)

# Load account_id and iam_role from the conf file
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
account_id = parser.get("aws_boto_credentials", "accound_id")
iam_role = parser.get("aws_creds", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

"""
If this is a full load, you will need to TRUNCATE the destination table
(clear the table, but leave defining factors and columns)

sql = "TRUNCATE public.Orders;"
cur = rs_conn.cursor()
cur.execute(sql)

cur.close()
rs_conn.commit()
"""

# Run the COPY command to load file into Redshift
file_path = ("s3://"
             + bucket_name
             + "/order_extract.csv")
role_string = ("arn:aws:iam::"
               + account_id
               + ":role/"
               + iam_role)

sql = "COPY public.Orders"
sql = sql + " from %s"
sql = sql + " iam_role %s;"

# create a cursor object and execute the COPY
cur = rs_conn.cursor()
cur.execute(sql, (file_path, role_string))

# Close the cursor and commit the transaction
cur.close()
rs_conn.commit()

# Close the connection
rs_conn.close()
