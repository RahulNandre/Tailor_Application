import boto3
import sys
import json
import psycopg2
import pandas as pd
from decimal import Decimal
#############################################variable initialization#############################
#database credentials
host    = 'testtailor.ctwfk2zxzjto.ap-south-1.rds.amazonaws.com'
port    =  5432
user    = 'tailordb'
password= 'tailorapp'
database= 'tailor'

#AWS service credentials
dynamodb_config = boto3.resource('dynamodb')
dynamodb_table  = dynamodb_config.Table('customers')

db_status = False
try:
    print("connecting to database...")
    connection = psycopg2.connect(
        host     = host,
        port     = port,
        user     = user,
        password = password,
        database = database
        )
    cursor=connection.cursor()
    db_status = True
    print("successfully connected to database!")
except Exception as e:
    print("error while connecting to database "+str(e))
################################################################################################

def delete_records(data):
    try:
        print("deleting records ids "+str(list(data.id)))
        data = list(data.id)
        for i in data:
            # "DELETE FROM parts WHERE part_id = %s", (part_id,)
            query= """delete from tailor.tailor_default_customers where id=%s;"""
            cursor.execute(query,str(i))
            connection.commit()
            print(str(i)+" id deleted")
        cursor.close()
        connection.close()
    except Exception as e:
        print("error while deleting records "+str(e))


#function to insert data into dynamodb
def put_item_in_database(data):
    #API expect data in dictionary format
    print(data)
    try:
        print("inserting data into dynamodb table")
        with dynamodb_table.batch_writer() as batch:
            for index, row in data.iterrows():
                batch.put_item(json.loads(row.to_json(), parse_float=Decimal))
        print("customers has been successfully inserted into dynamodb")
        delete_records(data)
        # get_item()
    except Exception as e:
        print("error while inserting data into dynamodb "+str(e))


#function to read data from database
def read_data():
    print("reading data...")
    query = """ SELECT "id","user_name","password" from tailor.tailor_default_customers
                where "status"=false
            """
    try:
        data = pd.read_sql(query, con=connection)
        if len(data)>0:
            print(data)
            put_item_in_database(data)
        else:
            print('no new records found')
    except Exception as e:
        print("error while reading data "+str(e))


#execution point
def lambda_handler(event, context):
    #database connection
    if db_status:
        read_data()
    else:
        pass


lambda_handler(event=[],context=[])