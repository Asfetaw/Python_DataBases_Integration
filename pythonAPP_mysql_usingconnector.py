
#driver to connect MySQL Server
import mysql.connector as mysqlconnector
import csv

#custome function to load your credential , good practice not to hard code your credention in connection string 
from  mysql_credential import load_db_credential

#if credential is saved somewhere on computer an, this is to avoid hardcoded credential 
cred = load_db_credential()

#connet credential using <you - credential >
def db_connect (db_name, host = None, user = None , password = None ):
    
    
    try:
    
        connection  = mysqlconnector.connect(
                                    host = 'localhost', # if your server is hosted locally 
                                    user = cred['user'],   # use your credential 
                                    password = cred['password'], 
                                    database = db_name, 
                                    allow_local_infile = 1  # if you want to enable loading file from local storage
                                    )
        return connection
    except Exception as e:
        print(e)

def create_table(cursor):
    
   
    create_query = """CREATE TABLE zip_state (
                                zipcode     INT(255) NOT NULL PRIMARY KEY,
                                city        VARCHAR(255) NOT NULL,
                                county      VARCHAR(255) NOT NULL.
                                state       VARCHAR(255) NOT NULL
                            )"""
    
    cursor.execute("DROP TABLE IF EXISTS zip_state;")
    cursor.execute(create_query)
    
    return cursor
def read_csv_and_insert_to_table(cursor):
   
    #This is more guaranty
    #read csv file which is in the same directory
    with open("./customers.csv","r") as f:
        data = csv.reader(f)
        for row in data:
            #convert to tuple
            row_tuple = tuple(row)
            #inserting each row at a time ( lastname comes first in the csv file) 
            cursor.execute('INSERT INTO zip_state (zipcode, city, county, state) VALUES {}'.format(values),row_tuple)
   
   
    cursor.execute(q)
    

    return cursor

def print_first_10_records(cursor):
    cursor.execute('SELECT * from zip_state LIMIT 10')
    first_10_records = cursor.fetchall()
    
    for record in first_10_records:
        print(record)
        
    return cursor
    


#driver code 
if __name__ == "__main__":
    
    #create connection
    dbname = 'customer'
    connection = db_connect(dbname)
    cursor = connection.cursor()
    #Create Table
    cursor = create_table(cursor)
    
    #read csv
    cursor = read_csv_and_insert_to_table(cursor)
    
    #commit insertion data from csv file into mysql database under salesperson table
    connection.commit()
    
    #print the first 10 records 
    
    cursor = print_first_10_records(cursor)
    
    #close connection
    connection.close()
    