import os 
import datetime
import pandas as pd
import sys
from functions import (setlocale, test_existence_file)
import configparser

import logging
log = logging.getLogger(__name__)

def get_xls_paths(path):

    config                  = configparser.ConfigParser()
    config.read(path)
    bbva_principal_path     = config['EXPORT_FILES']['bbva_principal']
    bbva_prepago_path       = config['EXPORT_FILES']['bbva_prepago']
    lacaixa_path            = config['EXPORT_FILES']['lacaixa']

    return bbva_principal_path, bbva_prepago_path, lacaixa_path

class Error(Exception):
    pass

class OverlapTooSmall(Error):
    pass

class ImportFileNotFound(Error):
    pass

class AccountDetailsGetter:
    fecha_format = "%Y-%m-%d %H:%M:%S"
    def __init__(self, name,filepath):
        self.__name__ = name
        self.path = filepath
        print('\n\nStarting {} simulation\nUsing file: {}'.format(self.__name__,self.path))
        if not test_existence_file(self.path):
            raise ImportFileNotFound('Import file for source {} not found'.format(self.__name__),self.path)
        self.modification_date = datetime.datetime.fromtimestamp( os.stat(self.path).st_mtime )
        self.get_file_name()

    def get_file_name(self):
        temp = self.path[self.path.rfind('/')+1:]
        self.filename = temp[:temp.find('.')]

    def check_duplicates(self,output):
        log.info('Checking duplicates')
        self.add_column('DUPLICADO',0)
        duplicated_rows = self.df[self.df.duplicated()]
        output = output +'.csv'
        if not duplicated_rows.empty:
            log.warning('Found duplicated entries!, printing them into file: {}'.format(output))
            duplicated_rows = duplicated_rows.groupby(duplicated_rows.columns.tolist()).size().reset_index().rename(columns={0:'records'})
            duplicated_rows.to_csv(output,index=False)

            log.info('Inserting duplicated rows with new DUPLICADO value')
            self.df.drop_duplicates(inplace=True)
            for index,row in duplicated_rows.iterrows():
                records = row['records']
                row.drop('records',inplace=True)
                for i in range(records):
                    row['DUPLICADO'] = i+1
                    self.df = self.df.append(row,ignore_index=True)
            
            duplicated_rows = self.df[self.df.duplicated()]
            if not duplicated_rows.empty:
                raise ValueError('Duplicated rows exist after checking them')

    def set_table_style(self,sheet_name=0,header_row=None,columns_range=None):
        log.info('Setting table style')
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.columns_range = columns_range
        
    def xls_to_pandas(self,na_filter=None):
        log.info('Extracting xls into pandas dataframe')
        self.df = pd.read_excel(self.path, sheet_name=self.sheet_name, header=self.header_row, usecols = self.columns_range,na_filter=na_filter)
        
    def add_column(self,column,value):
        log.info('Adding column: {} with value: {}'.format(column,value))
        self.df[column] = value
            
    def set_fk_origen(self,db_handler):
        self.fk_origen = db_handler.get_pk_from_columns(['DESC_ORIGEN'],[self.__name__], 'DESC_ORIGEN')
        log.info('FK for source: {}'.format(self.fk_origen))
        self.add_column('FK_ORIGEN',self.fk_origen)
        
    def set_fk_documento_importacion(self,db_handler):
        columns = ['FK_ORIGEN','FECHA']
        table = 'DESC_DOCUMENTO_IMPORTACION'
        values = [self.fk_origen, self.modification_date.strftime(self.fecha_format)]
        self.fk_documento_importacion = db_handler.insert_row_get_id(columns,values,table,get_pk_if_exists=True)
        log.info('FK for import document: {}'.format(self.fk_documento_importacion))
        self.add_column('FK_DOCUMENTO_IMPORTACION',self.fk_documento_importacion)
        
    def set_fk_insercion(self,db_handler,now):
        self.fk_insercion = db_handler.insert_row_get_id(['FECHA'],[now.strftime(self.fecha_format)],'DESC_INSERCIONES')
        log.info('FK for insertion time: {}'.format(self.fk_insercion))
        self.add_column('FK_INSERCION',self.fk_insercion)
    
    def put_modification_date_in_pandas(self):
        log.info('Putting modification date into dataframe')
        self.add_column('Fecha modificación documento',self.modification_date)
        
    def change_column_name(self,column,new_column):
        log.info('Renaming column: {} into: {}'.format(column,new_column))
        self.df = self.df.rename(columns= {column : new_column})
        
    def remove_file(self):
        print('Finished importing {}, removing file...'.format(self.__name__))
        os.remove(self.path)
    
    def from_str_to_datetime(self,column, str_format, locale=None):
        log.info('Fixing str into datetime')
        with setlocale(locale):
            self.df[column] = pd.to_datetime(self.df[column], format= str_format)
                
    def delete_rows_if_column(self, column_name, column_value):
        log.info('Deleting rows where column: {} has value: {}'.format(column_name, column_value))
        self.df = self.df.drop(self.df[self.df[column_name] == column_value].index)  
        
    def remove_rows_already_in(self,db_handler):
        log.info('Removing the rows that are already in the database')
        # I'll get from the database the rows newer than the oldest item in xls.
        min_time = self.df.min(axis=0)['FECHA']
        query = 'SELECT FECHA,CONCEPTO,MASDATOS,IMPORTE,SALDO FROM LOG_GASTOS WHERE FECHA >= "{}" AND FK_ORIGEN = "{}"'.format(min_time.strftime(self.fecha_format),self.fk_origen)
        db_df = db_handler.query_to_pandas(query)
        # Add column fk_origen to match exactly.
        db_df['FK_ORIGEN'] = self.fk_origen
        
        # Get previous len to check eliminated values
        self.previous_len_df = len(self.df)
        self.df = pd.merge(self.df, db_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
        
    def check_overlap(self,db_handler):
        # Check overlap
        log.info('Checking overlap')
        overlap = - len(self.df) + self.previous_len_df
        if overlap < 2:
            print('Overlap small ({}) for source: {}'.format(overlap,self.__name__))
            if db_handler.is_table_empty('LOG_GASTOS',filter_='WHERE (FK_ORIGEN = {})'.format(self.fk_origen)): 
                log.info('Table for source is empty, no problem')
            else:
                log.info('Table for source is not empty, overlap is a problem')
                raise OverlapTooSmall('Overlap should be low to avoid consistency problems',overlap,self.__name__)
        log.info('Overlapping elements: {}'.format(overlap))
    def to_database(self,db_handler):
        db_handler.insert_dataframe(self.df,'LOG_GASTOS')
        

class UnifiedDetails:
    def __init__(self,columns):
        self.df = pd.DataFrame(columns = columns) 
        
    def append_dataframes(self,dataframes):
        for dataframe in dataframes:
            if set(self.df.columns) != set(dataframe.columns):
                raise ValueError('Dataframe {} does not have the correct columns'.format(dataframe.__name__))
            else:
                self.df = pd.concat([self.df,dataframe],sort=True)
               
        