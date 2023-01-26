#from flask_sqlalchemy import SQLAlchemy
import sqlalchemy as sql
import pandas as pd
from sqlalchemy.engine import URL
import pyodbc

class SQLServer(object):
    def __init__(self,driver,server,uid,pwd,database):
        instance = ''
        if instance == '':
            connection_string = "DRIVER={"+driver+"};SERVER="+server+";DATABASE="+database+";UID="+uid+";PWD="+pwd
        else:
            connection_string = "DRIVER={"+driver+"};SERVER="+server+";INSTANCE="+instance+";DATABASE="+database+";UID="+uid+";PWD="+pwd
        connection_url = URL.create("mssql+pyodbc", query ={"odbc_connect": connection_string})
        self.engine = sql.create_engine(connection_url, fast_executemany=True)

    def writeDataFrame_append(self,table,df,schema='dbo', chunksize = 10000 ):
        self.engine.connect()
        df.to_sql(name=table, con=self.engine, if_exists='append', index=False, schema=schema, chunksize = chunksize)
        #self.engine.Connection.close()   <--------- Deprecated? Auto close for each block now?

    def writeDataFrame_createTable(self,table,df,schema='dbo',index=False, index_label = 'ID', chunksize = 10000 ):
        self.engine.connect()
        df.to_sql(name=table, con=self.engine, if_exists='fail', index=index, schema=schema, chunksize = chunksize )

    def writeDataFrame_dropAndCreateTable(self,table,df,schema='dbo',index=False, index_label = 'ID', chunksize = 10000 ):
        self.engine.connect()
        df.to_sql(name=table, con=self.engine, if_exists='replace', index=index, schema=schema, chunksize = chunksize )

    def executeCustomSelect(self,query):
        queryString = sql.text(query)
        result = self.engine.execute(queryString)
        return result

    def executeCustomQuery(self,query):
        queryString = sql.text(query)
        self.engine.execute(queryString)
