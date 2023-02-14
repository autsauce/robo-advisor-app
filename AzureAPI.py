import pandas as pd
import requests
import platform
import pymssql
import pyodbc
import os

def initiate_API(live=False):
    return Azure(live)

class Azure(object):
    def __init__(self,live:bool=False):

        self.live = live

        self._env = self._determine_environment()
        self._whitelist_ip()

        if self._env == 'GoogleColab':
            self._cnxn = self._connect_via_OBDC()
        elif self._env == 'PythonAnywhere':
            self._cnxn = self._connect_via_FreeTDS_local()
        else:
            raise Exception('_determine_environment() did not return GC or PA.')

        self.cursor = self._cnxn.cursor(as_dict=True)

    def _whitelist_ip(self):

        ip = str(requests.get('https://api.ipify.org').text)
        name = self._env

        os.system('az sql server firewall-rule update --resource-group gcapp --server gc-default --name ' + name + ' --start-ip-address ' + ip + ' --end-ip-address ' + ip)
        return print(ip,'has been whitelisted.')

    def _determine_environment(self):

        pltfrm = platform.platform()

        if 'aws' in pltfrm:
            env = 'PythonAnywhere'
        elif 'Ubuntu' in pltfrm:
            env = 'GoogleColab'
        elif 'arm64-arm-64bit' in pltfrm:
            env = 'Local'
        else:
            raise Exception('Platform did not contain one of the expected strings.')
        
        return env

    def _connect_via_OBDC(self):

        driver= '{ODBC Driver 17 for SQL Server}'
        server = 'server'
        database = 'db'
        username = 'username'
        password = 'pass' 

        return pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

    def _connect_via_FreeTDS_local(self):

        server = 'server'
        database = 'db'
        username = 'username'
        password = 'pass' 

        return pymssql.connect(server=server, user=username, password=password, database=database)

    def _connect_via_FreeTDS_server(self):

        os.environ["ODBCSYSINI"] = "/home/user"

        dsn = 'dsn'
        db = 'db'
        uid = 'uid'
        pwd = 'pwd'
        enc = 'enc'
        to = 'Connection Timeout=30;'

        connection_string = dsn + db + uid + pwd + enc + to

        return pyodbc.connect(connection_string)

    def list_tables(self):
        self.cursor.execute("SELECT table_name FROM information_schema.tables")
        return [row['table_name'] for row in self.cursor.fetchall()]

    def retrieve_table(self,table_name:str = None):
        hybrid_tables = ['historicalData','assetMap']
        if (self.live) and (table_name not in hybrid_tables):
            table_name = 'LE_' + str(table_name).upper()
        else:
            table_name = str(table_name).upper()
        sql = 'SELECT * FROM {table_name}'.format(table_name=table_name)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if not result:
            df = pd.DataFrame([],columns=[item[0] for item in self.cursor.description])
        else:
            df = pd.DataFrame(result)
        return df

    def retrieve_table_value(self,table_name:str = None,column_name:str = None,key_name:str = None,key_value:str = None):
        df = self.retrieve_table(table_name).set_index(key_name)[column_name]
        return df.loc[key_value]

    def update_table_value(self,table_name:str = None,column_name:str = None,column_value:str = None,key:str = None,key_value:str = None):
        if self.live:
            table_name = 'LE_' + str(table_name).upper()
        else:
            table_name = str(table_name).upper()

        column_name = str(column_name)
        column_value = str(column_value)
        key = str(key)
        key_value = str(key_value)

        if key_value != 'NULL':
          sql = "UPDATE {table_name} SET {column_name} = '%s' WHERE {key} = '%s'".format(table_name=table_name,column_name=column_name,key=key)
          self.cursor.execute(sql % (column_value,key_value))
        else:
          sql = "UPDATE {table_name} SET {column_name} = '%s' WHERE {key} IS NULL".format(table_name=table_name,column_name=column_name,key=key)
          self.cursor.execute(sql % column_value)
        return print('Updated ' + table_name + ' table. Set ' + column_name + ' to ' + column_value + ' where ' + key + ' equals ' + key_value + '.')

    def delete_table_row(self,table_name:str = None,identifier:str = None,value:str = None):
        if self.live:
            table_name = 'LE_' + str(table_name).upper()
        else:
            table_name = str(table_name).upper()

        value = str(value)
        sql = "DELETE FROM {table_name} WHERE {identifier} = '%s'".format(table_name=table_name,identifier=identifier)
        self.cursor.execute(sql % value)
        return print('Deleted rows from ' + table_name + ' where ' + identifier + ' was equal to ' + str(value) + '.')

    def insert_table_row(self,table_name:str = None,values:dict = None):
        if self.live:
            table_name = 'LE_' + str(table_name).upper()
        else:
            table_name = str(table_name).upper()

        sql = "INSERT into " + table_name + " ("

        for i in values:
          sql = sql + i + ','

        sql = sql[:-1] + ") values ("

        for i in values:
          v = values[i]
          sql = sql + "'" + str(v) + "'" + ","

        sql = sql[:-1] + ")"

        self.cursor.execute(sql)

    def delete_table_column(self,table_name:str = None,column_name:str = None):
        if self.live:
            table_name = 'LE_' + str(table_name).upper()
        else:
            table_name = str(table_name).upper()
        self.cursor.execute("ALTER TABLE " + table_name + " DROP COLUMN " + column_name)

    def insert_table_column(self,table_name:str = None,column_name:str = None,data_type:str = None):
        if self.live:
            table_name = 'LE_' + str(table_name).upper()
        else:
            table_name = str(table_name).upper()
        self.cursor.execute("ALTER TABLE " + table_name + " ADD " + column_name + " " + data_type)
        
    def commit_changes(self):
        self._cnxn.commit()

    def close_connection(self):
        self.cursor.close()
        return print('The connection to the database has been closed.')

    def get_user_info(self,id:str = None):
        users = self.retrieve_table('users')
        accounts = self.retrieve_table('accounts')
        df = pd.merge(left=users,right=accounts,left_on='ID',right_on='user_id').set_index('alpaca_account_id')
        return df.loc[id]