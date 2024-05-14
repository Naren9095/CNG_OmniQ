from snowflake.snowpark.session import Session
from snowflake.snowpark import version
from snow_cred import snowflake_conn_prop

#creating the session in snowflake
session = Session.builder.configs(snowflake_conn_prop).create()

rolename = "ACCOUNTADMIN"
dbname = "OMNIQ"
schemaname = "RAW"
warehouse = "COMPUTE_WH"

session.sql(f"USE ROLE {rolename}").collect()
session.sql(f"USE WAREHOUSE {warehouse}").collect()
session.sql(f"USE SCHEMA {dbname}.{schemaname}").collect()

sql_query = session.sql('''select channel_id,channel_name,subscriber_count,total_views,join_date,videos_per_week
                           from youtube 
                           ''')
snow_df = sql_query.toPandas()
print(snow_df)