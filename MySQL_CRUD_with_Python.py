#interacting with MySQL database  using python 
#importing MySQL connecter driver
import mysql.connector as mysql
import csv

#import custom function to load user credential for the database
from  mysql_credential import load_db_credential



class InteractWithMySqlUsingPython:
    #connet credential using <you - credential >
    def __init__(self, db_name = 'dbname', host =None, user = None, password = None):
        """
        create a new connection based on :
            - database name
            - host : the address of MySQL server
            - user : user credential
            - password : user crdential to access the db
        """
        
        
        try:

            self.db_name = db_name
            self.host = host
            self.user = user
            self.password = password
        
            self.connection  = mysql.connect(
                                        host = self.host,
                                        user = self.user, 
                                        password = self.password, 
                                        database = self.db_name, 
                                        allow_local_infile = 1
                                        )
            
           
        except Exception as e:
            print(e)

    def create_table(self):
        """
        input:
            - self
        output
             - create operation : new table 
        
        """
        
        create_query = ''' CREATE TABLE salesperson(
                        id INT(255) NOT NULL AUTO_INCREMENT,
                        firstname VARCHAR(255) NOT NULL,
                        lastname VARCHAR(255) NOT NULL,
                        email_address VARCHAR(255) NOT NULL,
                        city VARCHAR(255) NOT NULL,
                        state VARCHAR(255) NOT NULL,
                        primary key(id) )'''
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS salesperson;")
        cursor.execute(create_query)

        self.connection.commit()
        
        return self
    def read_csv_and_insert_to_table(self, filename):
        """
        input:
            - csv file , shape (n_records , n_fields) 
        output
             - loading data from csv file into the table using csv reader
             - the number of column and dtype should matched with the target table
        Note :
            - n_fields should be the same with columns of the table
        """
        
        cursor = self.connection.cursor()
        
        #This is more guaranty
        #read csv file which is in the same directory
        path = os.path.abspath(filename)
        with open(f"{path}","r") as f:
            data = csv.reader(f)
            for row in data:
                #convert to tuple
                row_tuple = tuple(row)
                #inserting each row at a time ( lastname comes first in the csv file) 
                cursor.execute('INSERT INTO salesperson(firstname,lastname,email_address, city, state) VALUES("%s","%s","%s","%s","%s")',row_tuple)
        """
        #loading directly from file
        q = ''' LOAD DATA LOCAL INFILE 'C:/Users/asfetu/OneDrive/projects/Databases/mysql-csv-workspace/salespeople.csv'\
                INTO TABLE salesperson FIELDS TERMINATED BY ',' \
                ENCLOSED BY '"' (firstname, lastname, email_address, city, state);'''
        cursor = connection(cursor)
        cursor.execute(q)

        """

        #commit insertion data from csv file into mysql database under salesperson table
        self.connection.commit()
        
        

        return self


    def insert_record(self, *args):
        """
        input:
            - *args , type tuple , (firstname, lastname, email_address, city, state)
        output
             - add record  operation , add  (firstname, lastname, email_address, city, state) into newly created table
        """
        
        query = f'''
                   INSERT INTO salesperson(firstname, lastname, email_address, city, state)
                            VALUES {args};'''

               

        # delete if the same record is available
        
        
        cursor = self.connection.cursor()
        
        #delete record if exists
        
        self.delete_record(args[0], args[1])
        
        cursor.execute( query )
        self.connection.commit()

        #sanity check

        q = f''' SELECT * FROM salesperson WHERE firstname like "{args[0]}" ;'''
       
        cursor = self.connection.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        input("please hit any key to continue")
        print(" The following record is inserted as new record ")
        for row in result:
            print(row)
        
        return self


    def update_record(self, *args):
        """
        input:
            - *args , type tuple , (firstname , city , state)
        output
             - Update operation , update the city and state of the first name 
        """
        query = f"""
                UPDATE salesperson
                SET city = "{args[1]}" , state = "{args[2]}"
                WHERE firstname = "{args[0]}" ;
                
                """
        cursor = self.connection.cursor()
        cursor.execute(query)

        print("{0}'s city and state  are  updated  to {1} and {2}".format(args[0],args[1],args[2]))

        #commit update
        self.connection.commit()

        return
    def delete_record(self,*args):
        """
        input:
            - self
        output
             - Delete operation 
        """
        q = f''' DELETE FROM salesperson WHERE firstname = "{args[0]}" and lastname = "{args[1]}" ;'''
        cursor = self.connection.cursor()
        cursor.execute(q)
        self.connection.commit()
        
        

        return self
        
    def print_first_5_records(self):
        """
        input:
            - self
        output
             - Read operation 
        """
        cursor = self.connection.cursor()
        cursor.execute('SELECT * from salesperson LIMIT 5;')
        first_5_records = cursor.fetchall()
        if not first_5_records:
            print("Empty Records")
        else:
            for record in first_5_records:
                print(record)
            
        return self
    def __repr__(self):

        return f"your are working on``` {self.db_name} database ```  which is hosted at``` {self.host} ```"

#driver code 
if __name__ == "__main__":

    cred = load_db_credential()
    #create an object using < "dbname", "username" , "password" and "ip address" of MYSQL or "host" if the server  is installed  locally >
    my_object = InteractWithMySqlUsingPython(databse = 'sales', host = 'localhost', user = cred['user'], password = cred['password']) #change the arguments as per your setting
    
    print("Conection with database is created: \n{}".format(my_object))
    
    #let's Do CRUD operation
    #Create Table
    my_object.create_table()
    
    
    
    #insert new records
    while 'y' in input(f" Do you want to insert a new record (y/n) ").lower() :
        firstname = input("What is first name of the salesperson ?\n")
        lastname = input(f"What is the last name of the {firstname} ?\n")
        email_address = input(f"What is the email address of {firstname} ?\n")
        city = input(f"Where does {firstname} live (put only the city name)\n")
        state = input(f"In which state {firstname} is living (put only state name)\n")

        my_object.insert_record(firstname, lastname, email_address, city, state)

    #lets read the  first 10 records
    if 'y' in input(" Do you want to see the first five record (y/n) ").lower():
        
        print("Top 5 records of this table")
        my_object.print_first_5_records()
        

    #updating someone's city and state 
    my_object.update_record(firstname, "New York", "NY")
    
  
    
    # Delete records
    if 'y' in input(f" Do you want to delete the record of {firstname}(y/n) ").lower():
        my_object.delete_record(firstname, lastname)
        print(" {0}'s record is deleted from the database ....".format(firstname))

   
    #read csv
    
    filename = './salespeople.csv' # as example , put this file in the same directory
    #my_object.read_csv_and_insert_to_table()
    
    #close connection
    my_object.connection.close()
    
    
                    
