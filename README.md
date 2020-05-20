# DB_gastos

Herramienta para cargar datos de gastos de diferentes fuentes a una base de datos mysql.

## Uso

El script ``xls_getter.py`` carga los archivos exportados de la web directamente a la base de datos. 

## Consideraciones

Para evitar pasar por alto gastos duplicados, es necesario que cada carga contenga elementos ya cargados en la base de datos. En caso que no se quiera comprobar solapamiento se debe indicar ``check_overlap = False``. 

Path nos indica dónde buscar los archivos de carga, cada uno definido con comodines para buscar todas las posibles opciones. 
