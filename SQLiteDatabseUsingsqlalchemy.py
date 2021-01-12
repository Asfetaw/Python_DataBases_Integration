#create SQLite database using sqlalchemy 
import sqlalchemy
from sqlalchemy import create_engine, Column, INTEGER, TEXT, Table


class SQliteWithSqlalchemy:
    def __init__(self, database_name ):
        #create database if not exists
           
        self.database_name = database_name
        self.engine = create_engine(f'sqlite:///{self.database_name}')
        self.connection = self.engine.connect()
        self.metadata = sqlalchemy.MetaData()
        self.user = None
    

        self.metadata.create_all(self.engine)
            
    def create_table(self, table_name):
        """
        creating a new table
        input :
            - table_name , type string used to name the newly created table
        output:
            - new table will be created under the database 
        """

        query = f'''
                CREATE TABLE IF NOT EXISTS {table_name}
                (user_id INTEGER PRIMARY KEY  AUTOINCREMENT,
                 first_name TEXT,
                 second_name TEXT,
                 email_address TEXT)
                '''
        
        
        self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.connection.execute(query)
       

        return self

    def insert_record(self,  tablename , records):

        """
        input :
        - table_name , type string 
            - records , type list like array , it is a list of dictionaries

        output:
            - add  each element of the list into existing table 
        """

        # create a table instance 
       
        self.user = Table(table_name ,self.metadata,
                                Column("user_id", INTEGER, primary_key = True),
                                Column("first_name",TEXT),
                                Column("second_name",TEXT),
                                Column("email_address",TEXT))

        query = self.user.insert().values(records)

        
        self.connection.execute(query)

        return self

        

    def print_records(self):
        """
        print records from the instance table
        """

        select_query = sqlalchemy.select([self.user.columns.user_id,self.user.columns.first_name, self.user.columns.second_name,self.user.columns.email_address])
        #cursor = self.connection.cursor()
        result = self.connection.execute(select_query)

        result = result.fetchall()
        #print
        print("This table  has the following records ... ")
        for row in result:
            print(row)

        return self

    def __repr__(self):

        return f"<Databse(name = {self.database_name}) "

        
#driver code
if __name__ == "__main__":

    #initialize  database and table names
    database_name = "users.db"
    table_name = "user2"
    print("database name {} , tablename = {} are initialized  ".format(database_name,table_name))                        
    #create database connection
                                
    my_database = SQliteWithSqlalchemy(database_name)
    print("database connected sucessfully !!!")

    #create a table
    my_database.create_table(table_name)

    print("table create ...")

    #insert records
    #example 
    records = [
        {"first_name":"Asfetaw", "second_name" : "Abera", "email_address":"ggg@gmail.com"},
        {"first_name":"engineer1", "second_name" : "engineer1lastname", "email_address":"engineer1@gmail.com"},
        {"first_name":"engineer2", "second_name" : "engineer2lastname", "email_address":"engineer2@gmail.com"},
        {"first_name":"engineer3", "second_name" : "engineer3lastname", "email_address":"engineer3@gmail.com"}]
        
    my_database.insert_record(table_name, records)

    #print the first records
    my_database.print_records()

    

    my_database.connection.close()
                                
                                                         
