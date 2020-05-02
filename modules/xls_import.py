#!/usr/bin/env python
# coding. utf-8

import sys,os
from xls_part import AccountDetailsGetter, UnifiedDetails, OverlapTooSmall, ImportFileNotFound
from db_part import DBHandler 
import datetime

now = datetime.datetime.now()

# Overlap behaviour pass/raise 
def get_data_BBVA_principal(file_path,db_handler,check_overlap=True):
    try:
        BBVA_general = AccountDetailsGetter("BBVA principal",file_path)
        BBVA_general.set_table_style(sheet_name=0, header_row=4, columns_range="B,D:F,H")
        BBVA_general.xls_to_pandas()
        BBVA_general.set_fk_origen(db_handler)


        BBVA_general.change_column_name("Fecha","FECHA")
        BBVA_general.change_column_name("Importe","IMPORTE")
        BBVA_general.change_column_name("Concepto","CONCEPTO")
        BBVA_general.change_column_name("Movimiento","MASDATOS")
        BBVA_general.change_column_name("Disponible","SALDO")
        BBVA_general.delete_rows_if_column('CONCEPTO','Recarga de tarjetas prepago')
        BBVA_general.delete_rows_if_column('CONCEPTO','Descarga de tarjetas prepago')


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
        BBVA_prepago.set_fk_origen(db_handler)

        BBVA_prepago.change_column_name('Fecha','FECHA')
        BBVA_prepago.change_column_name('Concepto','CONCEPTO')
        BBVA_prepago.change_column_name('Importe','IMPORTE')
        BBVA_prepago.add_column("MASDATOS",None)
        BBVA_prepago.add_column("SALDO",None)
        BBVA_prepago.from_str_to_datetime("FECHA",str_format="%d/%m/%Y")
        BBVA_prepago.delete_rows_if_column('CONCEPTO','Bbva-net   carga tjt prepago c')
        BBVA_prepago.delete_rows_if_column('CONCEPTO','Bbva-net   descarga prepago  c')

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