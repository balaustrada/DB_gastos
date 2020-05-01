# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import sys,os
sys.path.insert(0,'modules')
from db_part import DBHandler, get_db_credentials
from xls_import import get_data_BBVA_principal, get_data_LaCaixa, get_data_BBVA_prepago
from xls_part import OverlapTooSmall, ImportFileNotFound
from functions import get_oldest_file



# %%
path = "/home/pi/DB/Gastos/Source/"
credentials_file = "/home/pi/DB/.DB_CREDENTIALS.ini"

# %%
bbva_file = get_oldest_file(path+'/bbva_principal*')
lacaixa_file = get_oldest_file(path+'/lacaixa*')
bbva_prepago_file = get_oldest_file(path+'/bbva_prepago*')


# %%
hostname, username, password, port, host = get_db_credentials(credentials_file)
db_handler = DBHandler(hostname = hostname, username = username, password = password,
    main_database = 'gastos_pre',port = port, host = host)     


# %%
if bbva_file:
    try:
        BBVA_general = get_data_BBVA_principal(bbva_file,db_handler,check_overlap=True)
        BBVA_general.to_database(db_handler)
        BBVA_general.remove_file()
    except OverlapTooSmall as e:
        print('Overlap too small: {}'.format(e))

# %%
if lacaixa_file:
    try:
        lacaixa = get_data_LaCaixa(lacaixa_file,db_handler,check_overlap=True)
        lacaixa.to_database(db_handler)
        lacaixa.remove_file()
    except OverlapTooSmall as e:
        print('Overlap too small: {}'.format(e))

# %%
if bbva_prepago_file:
    try:
        BBVA_prepago = get_data_BBVA_prepago(bbva_prepago_file,db_handler,check_overlap=False)
        BBVA_prepago.to_database(db_handler)
        BBVA_prepago.remove_file()
    except OverlapTooSmall as e:
        print('Overlap too small: {}'.format(e))


