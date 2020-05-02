#!/usr/bin/env python
# coding. utf-8

import sys,os
from xls_part import AccountDetailsGetter, UnifiedDetails, OverlapTooSmall, ImportFileNotFound
from db_part import DBHandler 
import datetime
import glob
import pandas as pd
import configparser
import logging
import shutil

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

now = datetime.datetime.now()
duplicates_output = "/home/pi/DB/Gastos/Output/"
mail_ini_file = "/home/pi/DB/.EMAIL.ini"

def get_mail(path):
    log.info('Getting mail from config file: {}'.format(path))
    config = configparser.ConfigParser()
    config.read(path)
    mail = config['EMAILS']['gastos']
    return mail

def unify_output(name):
    log.info('Unifying output')
    files = glob.glob(duplicates_output+name)
    df = pd.DataFrame()
    for file in files:
        df = df.append(pd.read_csv(file))
    if not df.empty:
        name = duplicates_output+name[:-1]+'.csv'
        log.info('Putting unified csv into file: {}'.format(name))
        df.drop_duplicates(inplace=True)
        df.to_csv(name,index=False)
        with open('temp.txt','w+') as f:
            f.write(df.to_string())
        send_file_through_mail(name, get_mail(mail_ini_file), 'possible duplicated', 'temp.txt')
        os.remove('temp.txt')
    return

def send_file_through_mail(file,mail,subject,body):
    log.info('Sending mail with attached file')
    log.debug('{} {} {} {}'.format(file,mail,subject,body))
    command = 'cat {} | mail -s "{}" {} -A {}'.format(body,subject, mail, file)
    os.system(command)


# Overlap behaviour pass/raise 
def get_data_BBVA_principal(file_path,db_handler,check_overlap=True):
    try:
        BBVA_general = AccountDetailsGetter("BBVA principal",file_path)
        BBVA_general.set_table_style(sheet_name=0, header_row=4, columns_range="B,D:F,H")
        BBVA_general.xls_to_pandas()
        BBVA_general.delete_rows_if_column('Concepto','Recarga de tarjetas prepago')
        BBVA_general.delete_rows_if_column('Concepto','Descarga de tarjetas prepago')
        BBVA_general.check_duplicates(duplicates_output+BBVA_general.filename)
        BBVA_general.set_fk_origen(db_handler)

        BBVA_general.change_column_name("Fecha","FECHA")
        BBVA_general.change_column_name("Importe","IMPORTE")
        BBVA_general.change_column_name("Concepto","CONCEPTO")
        BBVA_general.change_column_name("Movimiento","MASDATOS")
        BBVA_general.change_column_name("Disponible","SALDO")


        BBVA_general.remove_rows_already_in(db_handler)
        if check_overlap: BBVA_general.check_overlap(db_handler)
        BBVA_general.set_fk_documento_importacion(db_handler)
        BBVA_general.set_fk_insercion(db_handler,now)
        return BBVA_general
    except ImportFileNotFound as e:
        print(e.args[0])
        print("Given path is: {}".format(e.args[1]))
        raise
    except OverlapTooSmall as e:
        raise

def get_data_LaCaixa(file_path,db_handler,check_overlap=True):
    try:
        lacaixa = AccountDetailsGetter("LaCaixa",file_path)
        lacaixa.set_table_style(header_row=2,columns_range="A,C:F")
        lacaixa.xls_to_pandas(na_filter=False)
        lacaixa.check_duplicates(duplicates_output+lacaixa.filename)
        lacaixa.set_fk_origen(db_handler)

        lacaixa.change_column_name("Movimiento","CONCEPTO")
        lacaixa.change_column_name("Fecha","FECHA")
        lacaixa.change_column_name("Más datos","MASDATOS")
        lacaixa.change_column_name("Importe","IMPORTE")
        lacaixa.change_column_name("Saldo","SALDO")
        lacaixa.from_str_to_datetime("FECHA",str_format="%d %b %Y",locale="es_ES.utf8")

        lacaixa.remove_rows_already_in(db_handler)
        if check_overlap: lacaixa.check_overlap(db_handler)
        lacaixa.set_fk_documento_importacion(db_handler)
        lacaixa.set_fk_insercion(db_handler,now)
        return lacaixa
    except ImportFileNotFound as e:
        print(e.args[0])
        print("Given path is: {}".format(e.args[1]))
        raise
    except OverlapTooSmall as e:
        raise

def get_data_BBVA_prepago(file_path,db_handler,check_overlap=True):
    try:
        BBVA_prepago = AccountDetailsGetter("BBVA prepago",file_path)
        BBVA_prepago.set_table_style(header_row=4, columns_range="B,D:E")
        BBVA_prepago.xls_to_pandas()
        BBVA_prepago.delete_rows_if_column('Concepto','Bbva-net   carga tjt prepago c')
        BBVA_prepago.delete_rows_if_column('Concepto','Bbva-net   carga tjt prepago d')
        BBVA_prepago.delete_rows_if_column('Concepto','Bbva-net   descarga prepago  c')
        BBVA_prepago.check_duplicates(duplicates_output+BBVA_prepago.filename)
        BBVA_prepago.set_fk_origen(db_handler)

        BBVA_prepago.change_column_name('Fecha','FECHA')
        BBVA_prepago.change_column_name('Concepto','CONCEPTO')
        BBVA_prepago.change_column_name('Importe','IMPORTE')
        BBVA_prepago.add_column("MASDATOS",None)
        BBVA_prepago.add_column("SALDO",None)
        BBVA_prepago.from_str_to_datetime("FECHA",str_format="%d/%m/%Y")

        BBVA_prepago.remove_rows_already_in(db_handler)
        if check_overlap: BBVA_prepago.check_overlap(db_handler)
        BBVA_prepago.set_fk_documento_importacion(db_handler)
        BBVA_prepago.set_fk_insercion(db_handler,now)
        return BBVA_prepago
    except ImportFileNotFound as e:
        print(e.args[0])
        print("Given path is: {}".format(e.args[1]))
        raise
    except OverlapTooSmall as e:
        raise

