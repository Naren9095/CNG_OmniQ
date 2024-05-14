from sql_cred import server_name,database_name,user_name,password_name,driver_name
from sqlalchemy.engine import URL
from sqlalchemy import create_engine


try:
    connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password_name}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    print('Connection is successful')
except Exception as e:
    print(e)
