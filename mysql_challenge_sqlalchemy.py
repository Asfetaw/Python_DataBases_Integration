#import standard library
from sqlalchemy import Column, INTEGER, String, FLOAT, ForeignKey, create_engine, Integer, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import os

#connectio engine
engine = create_engine('mysql+mysqlconnector://root:Asf@@123@localhost:3306/red30')

#declarative
Base = declarative_base()

#create data model

class Red30(Base):
    __tablename__ = 'Sales'
    __table_args__ = {"schema":"red30"}
    
   
    order_num = Column(INTEGER, primary_key = True)
    order_type = Column(String(250))
    cust_name = Column(String(250))
    cust_state = Column(String(250))
    prod_category = Column(String(250))
    prod_number = Column(String(250))
    prod_name = Column(String(250))
    quantity = Column ( INTEGER)
    price = Column(FLOAT)
    discount = Column(FLOAT)
    order_total = Column(INTEGER)
    
    def __repr__(self):
        
        return ''' <Red30(order_num ={0}, order_type = {1} , cust_name = {2}, cust_state = {3},
                    prod_category = {4}, prod_number = {5}, prod_name = {6}, quantity = {7},
                    price = {8},discount = {9}, order_total = {10}'''.format(self.order_num, self.order_type, self.cust_name,
                                                                             self.cust_state, self.prod_category,self.prod_number,
                                                                                 self.prod_name,self.quantity, self.price, self.discount,         self.order_total)
    
#bind the engine to create a table 
Base.metadata.create_all(bind=engine)

#csv filename

filename = 'red30.csv'

df = pd.read_csv(os.path.abspath(filename))

df.to_sql(con = engine , name = Red30.__tablename__.lower(), if_exists = 'replace', index = False)

session = sessionmaker()
session.configure(bind=engine)
s = session()

#result = s.query(Red30).limit(10).all()
"""
for record in result:
    print(record)
"""    
    

#find the largest order total 

max_order = s.query(func.max(Red30.order_total)).scalar()

print("The maximum order is : {}".format(max_order))
s.commit()
s = session()
    
result = s.query(Red30).order_by(Red30.order_total.desc()).limit(10)

#print(list(result))
for record in result:
    print(record)

