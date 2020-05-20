# %%
import sys,os
import glob
import argparse
file_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0,file_path + '/modules')
from db_part import DBHandler, get_db_credentials
from xls_import import get_data_BBVA_principal, get_data_LaCaixa, get_data_BBVA_prepago, unify_output
from xls_part import OverlapTooSmall, ImportFileNotFound, get_xls_paths
from functions import get_oldest_file

import logging.config
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger(__name__)

# %%
parser = argparse.ArgumentParser()
parser.add_argument("-c","--config", required=False, type=str, help="Config file location", default=file_path + "/config_example.ini")
parser.add_argument("-d","--dbcred", required=False, type=str, help="Database credentials file location", default="/home/pi/DB/.DB_CREDENTIALS.ini")
parser.add_argument("-p","--pro", action='store_true', help="Use pro database")

args = parser.parse_args()

if args.pro:
    database = 'gastos_pro'
else:
    database = 'gastos_pre'

# %%
hostname, username, password, port, host = get_db_credentials(args.dbcred)
db_handler = DBHandler(hostname = hostname, username = username, password = password,
    main_database = 'gastos_pre',port = port, host = host)     

bbva_principal_path, bbva_prepago_path, lacaixa_path = get_xls_paths(args.config)

# %%
try:
    bbva_files = glob.glob(bbva_principal_path)
    for i,file in enumerate(sorted(bbva_files)):
        BBVA_general = get_data_BBVA_principal(file,db_handler,check_overlap=True)
        BBVA_general.to_database(db_handler)
        BBVA_general.remove_file()
    unify_output('bbva_principal*')
except OverlapTooSmall as e:
    print('Overlap too small: {}'.format(e))

# %%
try:
    lacaixa_files = glob.glob(lacaixa_path)
    for i,file in enumerate(sorted(lacaixa_files)):
        lacaixa = get_data_LaCaixa(file,db_handler,check_overlap=True)
        lacaixa.to_database(db_handler)
        lacaixa.remove_file()
    unify_output('lacaixa*')
except OverlapTooSmall as e:
    print('Overlap too small: {}'.format(e))

# %%
try:
    bbva_prepago_files = glob.glob(bbva_prepago_path)
    for i,file in enumerate(sorted(bbva_prepago_files)):
        BBVA_prepago = get_data_BBVA_prepago(file,db_handler,check_overlap=True)
        BBVA_prepago.to_database(db_handler)
        BBVA_prepago.remove_file()
    unify_output('bbva_prepago*')
except OverlapTooSmall as e:
    print('Overlap too small: {}'.format(e))


