from contextlib import contextmanager
import locale
import os
import glob

@contextmanager
def setlocale(name=None):
    if name:
        saved = locale.setlocale(locale.LC_ALL)
        try:
            yield locale.setlocale(locale.LC_ALL,name)
        finally:
            locale.setlocale(locale.LC_ALL,saved)
    else:
        yield None
        
def get_oldest_file(search_path):
    files = sorted(glob.glob(search_path))
    if files:
        return files[0]
    else:
        return None

def check_iterable(to_test):
    if isinstance(to_test,list) or isinstance(to_test,tuple) or isinstance(to_test,set):
        return True
    else: 
        return False
    
def check_consistency(columns,values):
    if len(columns) != len(values):
        raise ValueError('Columns and values have different shapes',columns,values)
    elif not check_iterable(columns) or not check_iterable(values):
        raise ValueError('Columns or values is not iterable',columns,values)
    else:
        return True
    
def check_existence_file(file):
    if os.path.isfile(file):
        return True
    else:
        return False