import pymysql
import sys
from modules.functions import check_consistency
from contextlib import contextmanager
import pandas as pd
from sqlalchemy import create_engine
import configparser
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

def get_db_credentials(path):
    config = configparser.ConfigParser()
    config.read(path)
    hostname = config['DB_CREDENTIALS']['hostname']
    password = config['DB_CREDENTIALS']['password']
    username = config['DB_CREDENTIALS']['username']
    port = int(config['DB_CREDENTIALS']['port'])
    host = config['DB_CREDENTIALS']['host']
    return hostname, username, password, port, host

class DBHandler:
    def __init__(self,hostname, username, password, main_database, port, host):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.main_database = main_database
        self.port = port
        self.host = host
        
    @contextmanager
    def set_connection(self):
        try:
            conn = pymysql.connect(host=self.host,user=self.username,
                          passwd=self.password, db=self.main_database, port = self.port)
            yield conn
            
        finally:
            conn.close()
            
    @contextmanager
    def set_insert_connection(self):
        try:
            conn = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(self.username,self.password,self.host,self.port,self.main_database), echo=False)
            yield conn
        finally:
            pass
            
    def query_to_pandas(self,query):
        with self.set_connection() as conn:
            return pd.read_sql(query,conn)
        
    def insert_dataframe(self,dataframe,table):
        with self.set_insert_connection() as conn:
            dataframe.to_sql(table,conn,if_exists='append',index=False)
            
    def get_values(self,db_cols,db_table):
        columns = ','.join([x for x in db_cols])
        
        try:
            conn = pymysql.connect(host=self.host,user=self.username,
                      passwd=self.password, db=self.main_database, port = self.port)
            
            cursor = conn.cursor()
            
            query = "SELECT {} from {}".format(columns, db_table)
            print(query)
            cursor.execute(query)
            result = cursor.fetchall()
            conn.close()
        
        except pymysql.Error as e: 
            print('Got error {!r}, errno is {}'.format(e,e.args[0]))
            print(query)
            conn.close()
            sys.exit('Error')
        return result

    def is_table_empty(self,table,filter_):
        log.info('Checking if table {} is empty'.format(table))
        results = self.run_query('SELECT * FROM {} {} LIMIT 1'.format(table,filter_))
        if not results:
            log.info('Table is empty')
            return True
        else:
            log.info('Table is not empty')
            return False

    def run_query(self,query,dictionary=False,show=False): 
        if dictionary: 
            use_cursor = pymysql.cursors.DictCursor
        else:
            use_cursor = pymysql.cursors.Cursor
            
        try:
            conn = pymysql.connect(host=self.host,user=self.username,
                      passwd=self.password, db=self.main_database, port = self.port, 
                                   cursorclass=use_cursor)
            
            cursor = conn.cursor()               
            cursor.execute(query)
            result = cursor.fetchall()
            conn.close()
            
        except pymysql.Error as e: 
            print('Got error {!r}, errno is {}'.format(e,e.args[0]))
            print(query)
            conn.close()
        return result
    
    def insert_values(self,db_cols,db_table,values):
        columns = ','.join([x for x in db_cols])
        entries = ', '.join(["%s" for i in range(len(db_cols))])
        
        try:
            conn = pymysql.connect(host=self.host,user=self.username,
                      passwd=self.password, db=self.main_database, port = self.port)
            
            cursor = conn.cursor()
            query = "INSERT INTO {} ({}) VALUES ({})".format(db_table,columns,entries)
            
            cursor.execute(query,values)
            rowid = cursor.lastrowid
            conn.commit()
            conn.close()
        
        except pymysql.Error as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            conn.rollback()
            conn.close()
            raise
        return rowid
    
    def get_pk_from_columns(self,columns, values, table):
        check_consistency(columns,values)
        
        where_clause = 'WHERE ' + ' AND '.join( ['{}="{}"'.format(columns[i],values[i]) for i in range(len(columns))] )
        query = 'SELECT PK_ID FROM {} '.format(table) + where_clause 
        
        result = self.run_query(query,dictionary=True)
        
        if len(result) == 0:
            raise ValueError('Found no row with query {}'.format(query))
        if len(result) > 1:
            raise ValueError('Found more than one row with query {}'.format(query))
        
        return result[0]['PK_ID']

    def insert_row_get_id(self,columns,values,table,get_pk_if_exists=True):
        check_consistency(columns,values)
        if get_pk_if_exists:
            try:
                pk = self.get_pk_from_columns(columns,values,table)
            except ValueError:
                pk = self.insert_values(columns,table,values)
        else:
            pk = self.insert_values(columns,table,values)
        
        return pk
                
    def insert_row_get_id_old(self,columns,values,table,get_pk_if_exists=False):
            check_consistency(columns,values)
            try:
                pk = self.insert_values(columns,table,values)
            except pymysql.err.IntegrityError:
                if get_pk_if_exists: pk = self.get_pk_from_columns(columns,values,table)
                else: raise
            return pk
