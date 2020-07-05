import unittest
from modules import functions as f
import os
import shutil
from datetime import datetime

class TestModule(unittest.TestCase):
    def setUp(self):
        # Creation of temporal files to later tests
        os.mkdir('tmp_path')
        open('tmp_path/file_1','a').close()
        open('tmp_path/file_2','a').close()

    def tearDown(self):
        # Clean temporal files
        shutil.rmtree('tmp_path')

    def test_check_existence_file(self):
        self.assertTrue(f.check_existence_file('tmp_path/file_1'))
        self.assertFalse(f.check_existence_file('tmp_path/file_30'))

    def test_check_consistency(self):
        col1    = [1,2,3]
        col2    = [4,5,5]
        col3    = [2,4,5,5]

        self.assertRaises(ValueError, f.check_consistency,col1,col3) 
        self.assertTrue(f.check_consistency(col1,col2))
        self.assertRaises(ValueError, f.check_consistency, 3 ,col2)

    def test_check_iterable(self):
        for iterable in ([0,1],(0,1),set([0,1])):
            self.assertTrue( f.check_iterable(iterable) )
        for non_iterable in (234,"hola",1):
            self.assertFalse( f.check_iterable(non_iterable) )

    def test_get_oldest_file(self):
        self.assertEqual( f.get_oldest_file('tmp_path/*'), 'tmp_path/file_1' )

    def test_setlocale(self):
        date = datetime(2009, 5, 20, 0, 0)

        with f.setlocale('es_ES.utf8'):
            string = date.strftime("%d%m%Y")
        
        self.assertEqual( string, '20052009' )


    
if __name__ == '__main__':
    unittest.main()
