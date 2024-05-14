import pyodbc
import pandas as pd
import sqlalchemy as sa

#imports from the py file
from sql_server_queries import q_user_schemas,schema_name,q_table_names
from sql_connection import engine


#------------------Connection details to connect to SQL Server-----------------------------Connections Page

# try:
#     myConnection = pyodbc.connect(
#     server = server_name,
#     database = database_name,
#     user = user_name,
#     password = password_name,
#     driver = driver_name
#     )
#     print(myConnection, ' is successful')
# except Exception as e:
#     print(e)

#--------------------------------------This gives the schema names and table names fromn the database----------------------------------------------------


#Schema names
with engine.begin() as conn:
    df = pd.read_sql_query(q_user_schemas, conn)
print(df)

#Table names
schema_name = list(df.schema_name)[1]     #selecting the schema name from UI
q_table_names = f'''select table_name from information_schema.tables where table_schema = '{schema_name}';'''
with engine.begin() as conn:
    df = pd.read_sql_query(q_table_names, conn)
print(df)

#Custom Query for the table
table_name = df.table_name[0]   #selecting the table name from UI
cust_query = f'''select channel_id,channel_name,subscriber_count,total_views,join_date,videos_per_week from {schema_name}.{table_name}'''

#preview data button
prev_query = 'select top 10 * from (' + cust_query + ') as a'
print(prev_query)
with engine.begin() as conn:
    df = pd.read_sql_query(prev_query, conn)
print(df)


# sql = '''select top 10 channel_name,subscriber_count,total_views,join_date,videos_per_week
# from [raw].[YOUTUBE]'''

# # cursor = myConnection.cursor()
# # cursor.execute(sql)
# # rows = cursor.fetchall()
# # for row in rows:
# #     print(row)

# #------------------reading in a DataFrame---------------------------------------------------
# df = pd.read_sql(sql, myConnection)
# myConnection.close()
# print(df)
#-------------------------------------------------------------------------------------------


