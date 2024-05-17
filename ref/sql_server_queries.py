q_user_schemas = '''
select s.name as schema_name
    --,s.schema_id,
    --u.name as schema_owner
from sys.schemas s
    inner join sys.sysusers u
        on u.uid = s.principal_id
where u.issqluser = 1
    and u.name not in ('sys', 'guest', 'INFORMATION_SCHEMA')'''

schema_name = ''
q_table_names = f'''select table_name from information_schema.tables where table_schema = '{schema_name}';'''
# print(table_names)


