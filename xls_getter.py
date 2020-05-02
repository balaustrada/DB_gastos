# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import sys,os
import glob
sys.path.insert(0,'/home/pi/DB/Gastos/Code/modules')
from db_part import DBHandler, get_db_credentials
from xls_import import get_data_BBVA_principal, get_data_LaCaixa, get_data_BBVA_prepago, unify_output
from xls_part import OverlapTooSmall, ImportFileNotFound
from functions import get_oldest_file

import logging.config
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger(__name__)

# Usage:
# This script will search in the path given for files with the corresponding extensions. It'll start appending them into the database and will stop either when all files are included or when the new file has no overlap with the database. (This feature can be deactivated with the check_overlap argument, but is desirable for consistency reasons.)


# %%
path = "/home/pi/DB/Gastos/Source/"
credentials_file = "/home/pi/DB/.DB_CREDENTIALS.ini"

# %%
bbva_files = glob.glob(path+'/bbva_principal*')
lacaixa_files = glob.glob(path+'/lacaixa*')
bbva_prepago_files = glob.glob(path+'/bbva_prepago*')


# %%
hostname, username, password, port, host = get_db_credentials(credentials_file)
db_handler = DBHandler(hostname = hostname, username = username, password = password,
    main_database = 'gastos_pre',port = port, host = host)     


# %%
try:
    for i,file in enumerate(sorted(bbva_files)):
        BBVA_general = get_data_BBVA_principal(file,db_handler,check_overlap=True)
        BBVA_general.to_database(db_handler)
        BBVA_general.remove_file()
    unify_output('bbva_principal*')
except OverlapTooSmall as e:
    print('Overlap too small: {}'.format(e))

# %%
try:
    for i,file in enumerate(sorted(lacaixa_files)):
        lacaixa = get_data_LaCaixa(file,db_handler,check_overlap=True)
        lacaixa.to_database(db_handler)
        lacaixa.remove_file()
    unify_output('lacaixa*')
except OverlapTooSmall as e:
    print('Overlap too small: {}'.format(e))

# %%
try:
    for i,file in enumerate(sorted(bbva_prepago_files)):
        BBVA_prepago = get_data_BBVA_prepago(file,db_handler,check_overlap=True)
        BBVA_prepago.to_database(db_handler)
        BBVA_prepago.remove_file()
    unify_output('bbva_prepago*')
except OverlapTooSmall as e:
    print('Overlap too small: {}'.format(e))


