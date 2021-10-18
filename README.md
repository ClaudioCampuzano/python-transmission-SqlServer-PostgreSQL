# python-transmission-SqlServer-PostgreSQL
Codigo para poder migrar ciertos datos de una tabla en SQLserver hacia postgreSQL

## Requisitos:
```
sudo apt install python3-pip
git clone https://github.com/ClaudioCampuzano/python-transmission-SqlServer-PostgreSQL && cd python-transmission-SqlServer-PostgreSQL
virtualenv env --python=python3 && source env/bin/activate
pip3 install -r requirements.txt
```

# Instalar el controlador ODBC de Microsoft para SQL Server (Linux)

https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15


## Ejecución:
```
source env/bin/activate
python3 main.py
```
