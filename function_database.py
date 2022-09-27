



domainDbUserName = "username"
domainDbPassword = "password"
domainSshUserName = "username"
domainSshPassword = "password"

#BEGIN TUNNEL SSH
port_ssh_tunnel=0
from sshtunnel import SSHTunnelForwarder
class tunnel_ssh():
    def __init__(self):
        self.__server = SSHTunnelForwarder(
        ('162.0.209.168',21098),
        ssh_username=domainSshUserName,
        ssh_password=domainSshPassword,

        remote_bind_address=('127.0.0.1', 3306)
        ) 
    def start(self):
        try:
            self.__server.start()
            return (self.__server.local_bind_port)  # show assigned local port
        except:
            print ("Error starting tunnel ssh")

    def stop(self):
        try:
            #work with `SECRET SERVICE` through `server.local_bind_port`.
            self.__server.stop()
        except:
            print("Error stoping tunnel ssh")
#ENF TUNNEL SSH



#BEGIN CONFIG SQLITE3*********************************************>

from os import close
import sqlite3
from typing import Coroutine
server_db_local_config_sqlite3_1 = {
    "server_db_name" : 'crud1',
    "server_db_system" : 'sqlite3',
    }

#sql_create_table_users_sqlite3 ='''CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, active INTEGER DEFAULT 1 NOT NULL, username VARCHAR(30) NOT NULL, age INTEGER NOT NULL, country VARCHAR(30) NOT NULL, phone VARCHAR(30) NOT NULL)'''
#sql_create_table_users_sqlite3 =("CREATE TABLE", "users", "(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, active INTEGER DEFAULT 1 NOT NULL, username VARCHAR(30) NOT NULL, age INTEGER NOT NULL, country VARCHAR(30) NOT NULL, phone VARCHAR(30) NOT NULL)")
# sql_create_table_products_sqlite3 =("CREATE TABLE", "products", "(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, active INTEGER DEFAULT 1 NOT NULL, productname VARCHAR(30) NOT NULL, price FLOAT NOT NULL)")
#sql_create_table_products_sqlite3 =("CREATE TABLE", "productos", "(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, active INTEGER DEFAULT 1 NOT NULL, username VARCHAR(30) NOT NULL, age INTEGER NOT NULL, country VARCHAR(30) NOT NULL, phone VARCHAR(30) NOT NULL)")

#END CONFIG SQLITE3*********************************************<



#BEGIN CONFIG MYSQL*********************************************>
import pymysql

server_db_local_config_mysql_1 = {
    "server_db_ip" : '127.0.0.1',
    "server_db_port" : 3306, #3306 is defaul port value of mysql
    "server_db_user" : 'jose',
    "server_db_password" : '',
    "server_db_name" : 'crud1',
    "server_db_system" : 'mysql'
    }

# server_db_local_config_mysql_2 = {
#     "server_db_ip" : '127.0.0.1',
#     "server_db_port" : 3306, #3306 is defaul port value of mysql
#     "server_db_user" : 'root',
#     "server_db_password" : '',
#     "server_db_name" : 'crud_python2',
#     "server_db_system" : 'mysql'
#     }




# tunnel = tunnel_ssh() #Use this line if gona use tunnel ssh
# port_ssh_tunnel = tunnel.start() #Use this line if gona use tunnel ssh
# print ('Port ssh: ' , port_ssh_tunnel)
# server_db_remote_ssh_tunnel_config_mysql_1 = {
#     "server_db_ip" : '127.0.0.1',
#     "server_db_port" : port_ssh_tunnel, #3306 is defaul port value of mysql local/ 5522 is port of namecheap / with tunnel ssh is server.local_bind_port
#     "server_db_user" : domainDbUserName,
#     "server_db_password" : domainDbPassword,
#     "server_db_name" : 'seguxjxs_crud_python_jpinto_1',
#     "server_db_system" : 'mysql',
#     }




# sql sentence to create a table
sql_create_table_users_mysql =("CREATE TABLE", "users", "(id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, active INTEGER DEFAULT 1 NOT NULL, username VARCHAR(30) NOT NULL, age INTEGER NOT NULL, country VARCHAR(30) NOT NULL, phone VARCHAR(30)  NOT NULL)")

#END CONFIG MYSQL*********************************************<




#BEGIN ALIAS FOR TABLE NAME**************************************************************************>
tabla_usuarios_mysql = "users"

#END ALIAS FOR TABLE NAME****************************************************************************<

#example 1 dict format for data in sql sentence

# datos_usuario = {
#     "username" : "'jose'",
#     "country" : "'venezuela",
#     "phone" : "'04143062185'",
#     "age" : "'44'"
# }

#example 2 dict format for data in sql sentence
#productos_table_name="product_pub"
# datos_productos ={
#     "product_name" : "'pelota playa'",
#     "product_price" : "'50'",
#     "product_description" : "'pelota de playa de colores'"  
# }
#example dict sql_statement for search all record
# sql_statement = {
#     "1":"1"
# }
#example dict sql_statement for search with 2 field with AND (id = 9 AND active = 2)
# sql_statement = {
#     "id":"9",
#     "active":"1"
# }

class database():
    def __init__(self,server_db_config):
        self.__server_db_name = server_db_config["server_db_name"]
        self.__server_db_system = server_db_config["server_db_system"]

        if self.__server_db_system == "mysql":
            self.__server_db_ip=server_db_config["server_db_ip"]
            self.__server_db_port=server_db_config["server_db_port"]
            self.__server_db_user = server_db_config["server_db_user"]
            self.__server_db_password = server_db_config["server_db_password"]

        self.__miConexion = ''
        self.__cur = ''
        self.status_conn = False
    
    def connect_db(self, create_db_value = 0):
        if (self.__server_db_system == "sqlite3"):
            try:           
                self.__miConexion=sqlite3.connect(self.__server_db_name)
                self.__cur=self.__miConexion.cursor()
                return (True, "Database '" + self.__server_db_name + "' was sucessfully open. ")
            except BaseException as err:
                return (False, "Error conecting to database '" + self.__server_db_name, err)
        elif (self.__server_db_system == "mysql"):
            try:                
                self.__miConexion = pymysql.connect(host=self.__server_db_ip, port=self.__server_db_port, user=self.__server_db_user, passwd=self.__server_db_password)
                sql = "SHOW DATABASES LIKE '" + self.__server_db_name + "'"
                self.__cur = self.__miConexion.cursor()
                self.__cur.execute(sql)
                result = self.__cur.fetchall()
                
                if (self.__server_db_name,) in result and create_db_value == 0: #Only opening database for other querys functions like insert, select, delete, update
                    self.close_db()
                    self.__miConexion = pymysql.connect(host=self.__server_db_ip, port=self.__server_db_port, user=self.__server_db_user, passwd=self.__server_db_password, db=self.__server_db_name)
                    self.__cur = self.__miConexion.cursor()
                    return (True, "Database '" + self.__server_db_name + "' was sucessfully open. ")
                elif (self.__server_db_name,) in result and create_db_value == 1: #Catches when is trying to create database that already exist
                    self.close_db()
                    return (False, "Database '" + self.__server_db_name + "' already exist. ")
                elif (self.__server_db_name,) not in result and create_db_value == 1: #Catches when is trying to create a new database and name is available
                        return (True, "Database '" + self.__server_db_name + "' is able to create. ")    
            except BaseException as err:
                return (False, "Error conecting to database '" + self.__server_db_name, err)
        else:
            return (False, "System DB name wrong or is missing. '" + self.__server_db_system + "'")

    def close_db(self):
        try:
            self.__cur.close()
        except BaseException as err:
            print ("Except closing Cursor. Detail: ", err)
        try:
            self.__miConexion.close()
        except BaseException as err:
            print ("Except closing DB. Detail: " , err)
            
    def verify_table_exist(self,table_name):
        
        if self.__server_db_system == 'mysql':
            sql = "SHOW TABLES LIKE '" + table_name + "'"
        elif self.__server_db_system == 'sqlite3':    
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name ='" + table_name + "'"
        self.__cur.execute(sql)
        result = self.__cur.fetchall()
        if (table_name,) in result:
            return (True,"Table '" + table_name + "' already exist. ")
        else:
            return (False,"Table '" + table_name + "' do not exist in database '" + self.__server_db_name + "'")

    def create_db(self):
        create_db_value = 1 #This value is used in connect() function to evaluate create a new database.
        if (self.__server_db_system == 'mysql'):
            try:
                connect = self.connect_db(create_db_value)
                if True in connect:
                    sql = "CREATE DATABASE " + self.__server_db_name
                    print(sql)
                    self.__cur.execute(sql)
                    self.close_db()
                    return("Database '" + self.__server_db_name + "' created succesfully. " + self.__server_db_system)
                else:
                    self.close_db()
                    return ("Creating database failed. " , connect[1], self.__server_db_system)
            except BaseException as err:    
                self.close_db()
                return("Error creating database", self.__server_db_name,self.__server_db_system, err)
        
        elif (self.__server_db_system == 'sqlite3'):
            try:
                open_db = open(self.__server_db_name)
                if open_db:
                    msg = "Database '" + self.__server_db_name + "' already exists. "
            except BaseException as err:
                connect = self.connect_db()
                msg = "The database '"+ self.__server_db_name +"' was created suscessfully. ", err
        self.close_db()
        return (msg,)

    def create_table(self,sql_create_table):
        table_name = sql_create_table[1]
        try:
            if self.__server_db_system == 'sqlite3':
                database_file = open(self.__server_db_name)
                database_file.close()
        except BaseException as err:
            return ("Error creating table '" + table_name + "'. database do not exist", self.__server_db_name,self.__server_db_system, err)
        try:
            connect = self.connect_db()
            if True in connect: 
                verify_table_ex = self.verify_table_exist(table_name)
                if True in verify_table_ex:
                    return ("Error creating table '" + table_name + "'. Alread exist." , connect[1],self.__server_db_system)
                else:
                    try:
                        
                        sql=' '.join(sql_create_table)
                        self.__cur.execute(sql)                    
                        return("Table '" + table_name + "' created succesfully. in '" + self.__server_db_name + "' " + self.__server_db_system)
                    except BaseException as err:
                        return ("Error creating table '" + table_name + "' ", sql_create_table, self.__server_db_system,self.__server_db_name, err)
            else:
                return ("Error creating table '" + table_name + "'. " , connect[1],self.__server_db_system)
        except BaseException as err:
            return ("Error creating table '" + table_name + "' ", sql_create_table, self.__server_db_system,self.__server_db_name, err)
        finally:
            self.close_db()

    #BEGIN SELECT*************************************************
    def select_from_table(self,table_name, sql_statement):
        try:
            table_name=str(table_name)
            if ((type(sql_statement)==dict) and (sql_statement!="") and (table_name!="")):
                sql = "SELECT * FROM " + table_name + " WHERE " + ' and '.join('{} = {}'.format(key, value) for key, value in sql_statement.items())
            elif((type(sql_statement)==str) and (sql_statement!="") and (table_name!="")):
                sql = sql_statement
            else:
                return ("error","1 or more args are missing or incomplete", self.__server_db_system,self.__server_db_name)
            connect = self.connect_db()
            if connect:
                try:
                    self.__cur.execute(sql)
                except BaseException as err:
                    return ("error","Error in SQL:> ", sql, self.__server_db_system,self.__server_db_name, err)
                result = self.__cur.fetchall()
                self.close_db()
                if result == ():
                    return ("error","No matches found in table", self.__server_db_system,self.__server_db_name)
                else:
                    return result
            else:
                return ("error", "Error conecting to database", self.__server_db_system,self.__server_db_name)
        except BaseException as err:
            return("Something went wrong",self.__server_db_system,self.__server_db_name, err)
        finally:
            self.close_db()
    #END SELECT*****************************************************************

    
    #BEGIN DELETE*************************************************
    def delete_from_table(self,table_name, sql_statement):
        table_name=str(table_name)
        if ((type(sql_statement)==dict) and (sql_statement!="") and (table_name!="")):
            sql = "SELECT * FROM " + table_name + " WHERE " + ' and '.join('{} = {}'.format(key, value) for key, value in sql_statement.items())
        elif((type(sql_statement)==str) and (sql_statement!="") and (table_name!="")):
            sql = sql_statement
        else:
            return ("error","1 or more args are missing or incomplete", self.__server_db_system,self.__server_db_name)
        if ((table_name!="") and (type(sql_statement==dict)) and (sql_statement!="")):
            connect = self.connect_db()
            if connect:
                verify_table_ex = self.verify_table_exist(table_name)
                if True in verify_table_ex:
                    sql = "DELETE FROM " + table_name + " WHERE " + ' and '.join('{} = {}'.format(key, value) for key, value in sql_statement.items())
                    try:
                        self.__cur.execute(sql)
                        self.__miConexion.commit()
                    except BaseException as err:
                        return ("error","Error in SQL:> ", sql, self.__server_db_system,self.__server_db_name, err)
                    result = self.__cur.rowcount
                    self.close_db()
                    if result == 0:
                        return ("error","No matches found in table " + table_name, self.__server_db_system,self.__server_db_name)
                    else:
                        return (result," record(s) deleted in table '" + table_name + "'", self.__server_db_system,self.__server_db_name)
                else:
                    self.close_db()
                    return (verify_table_ex[1])
            else:
                return ("error", "Error conecting to database", self.__server_db_system,self.__server_db_name)
        else:
            return ("error","1 or more args are missing or incomplete", self.__server_db_system,self.__server_db_name)
    #END DELETE*****************************************************************


    #BEGIN INSERT*************************************************
    def insert_row_table(self,table_name,table_data):
        #arg active = values that are partially errased from system and is optional, values 0 or 1)
        msg = ""
        table_name=str(table_name)
        if ((table_data!="") and (type(table_data==dict))(table_name!="")):
            connect = self.connect_db()
            if connect:
                verify_table_ex = self.verify_table_exist(table_name)
                if True in verify_table_ex:
                    sql = "INSERT INTO " + table_name + " (" + ",".join(list(table_data.keys())) + ") VALUES (" + ",".join(list(table_data.values())) + ")"
                    try:
                        self.__cur.execute(sql)
                        self.__miConexion.commit()
                    except BaseException as err:
                        return ("error","Error in SQL:> ", sql, self.__server_db_system, self.__server_db_name, err)
                    result = ("1 record inserted in table '" + table_name + "', ID:", self.__cur.lastrowid, self.__server_db_system,self.__server_db_name)
                    self.close_db()
                    return result
                else:
                    self.close_db()
                    return (verify_table_ex[1])
            else:
                return ("error", "Error conecting to database",self.__server_db_system,self.__server_db_name)
        else:
            return ("error","1 or more args are missing or incomplete",self.__server_db_system,self.__server_db_name)
    #END INSERT*****************************************************************

    #BEGIN UPDATE*************************************************
    def update_from_table(self,sql_statement,table_name,table_data=False):
        #arg active = values that are partially errased from system and is optional, values 0 or 1)
        table_name=str(table_name)
        if((type(sql_statement)==str) and (sql_statement!="") and (table_name!="") and type(table_data)==dict and table_data):
            sql = "UPDATE " + table_name + " SET " + ' , '.join('{} = {}'.format(key, value) for key, value in table_data.items()) + " WHERE " + sql_statement
        elif((type(sql_statement)==str) and (sql_statement!="") and (table_name!="")):
            sql = sql_statement
        else:
            return ("error","1 or more args are missing or incomplete",self.__server_db_system,self.__server_db_name)
        if self.connect_db():
            verify_table_ex = self.verify_table_exist(table_name)
            if True in verify_table_ex:
                try:
                    self.__cur.execute(sql)
                    self.__miConexion.commit()
                except BaseException as err:
                    return ("error","Error in SQL:> ", sql, err)
                result = (self.__cur.rowcount, "record(s) affected in table '" + table_name + "'" , self.__server_db_system,self.__server_db_name)
                self.close_db()
                return result
            else:
                self.close_db()
                return (verify_table_ex[1])
        else:
                return ("error", "Error conecting to database", self.__server_db_system,self.__server_db_name)
    #END UPDATE*****************************************************************




# TESTING EXAMPLES FOR ALL FUNCTIONS OF SQL





#BEGIN SQLITE3 +++++++++++++++++++++++++++++++++++++++++++++++

# db_1 = database(server_db_local_config_sqlite3_1) # local database conection


db_2 = database(server_db_local_config_mysql_1) # local database conection


# db_3 = database(server_db_remote_ssh_tunnel_config_mysql_1) # remote dababase conection



#*******************************************************
# Create Database
# createdb1 = db_1.create_db()
# print(createdb1)

# createdb2 = db_2.create_db()
# print(createdb2)


#*******************************************************

#Crear tablas
# create_table = db_1.create_table(sql_create_table_users_sqlite3)

# create_table = db_2.create_table(sql_create_table_users_mysql)

# create_table = db_3.create_table(sql_create_table_users_mysql)


# print(create_table)

#*******************************************************
#Parametros para insertar registros
table_name = "users" #Nombre de la tabla

#Dict que contiene los datos que serán registrados en la tabla. "Nombre de campo" : "Valor" / "field name" : "value"
table_data = {
    "username" : "'huse'",
    "country" : "'Uruguay'",
    "phone" : "'774433347'",
    "age" : "'30'"
}

# query_insert = db_1.insert_row_table(tabla_usuarios,datos_usuario)
# print(query_insert_1)

# query_insert = db_2.insert_row_table(tabla_usuarios,datos_usuario)
# print(query_insert)

# for i in range(10):
#     query_insert_1 = db_2.insert_row_table(tabla_usuarios,datos_usuario)
#     print(query_insert_1)

#*******************************************************

#ejemplos de parametros de querys simples: afecta SELECT, UPDATE, DELETE.

# sql_statement = {
#     "id" : "1",
# }

#sql_statement = "SELECT * FROM users WHERE id BETWEEN 5 AND 10"

sql_statement = {}

# sql_statement = {
#     "id" : "3",
# }



# sql_statement = {
#     "id" : "1",
#     "active" : "1"
# }

# sql_statement = {
#     "active":"1"
# }

# sql_statement = {
#      "1":"1"
# }



# table_data = {
#     "username" : "'y44epeto'",
# }
sql_statement = "id = 3"

#query_select_1 = db_1.select_from_table(tabla_usuarios,sql_statement)
#print(query_select_1)

# query_select= db_2.select_from_table(tabla_usuarios,sql_statement)
# print(query_select)

# query_delete = db_2.delete_from_table(tabla_usuarios,sql_statement)
# print(query_delete)




#END SQLITE3--------------------------------------------------

# tunnel.stop()









##############################################################################################
########################         BEGIN UPDATE EXAMPLES           #############################

# EXAMPLE 1: sql_statement as str will be readed as the condition of how many records will be updated
'''
table_name = "users" # This is the table that will be affected
sql_statement = "id = 3 or id = 2" # this var will be the condition as is written
table_data = { # This is a dict represent { columns : value } to be updated 
    "username" : "'333'",
    "country" : "'Uruguay'",
    "phone" : "'774433347'",
    "age" : "'30'"
}
query = db_2.update_from_table(sql_statement,table_name,table_data)
print (query)
'''
# END EXAMPLE 1


# EXAMPLE 2: sql_statement must be the complete sql sentence with all columns and values
'''
table_name = "users" # This is the table that will be affected
sql_statement = "UPDATE users SET username = 'Jossse', country = 'Uruguay' WHERE id = 3"
query = db_2.update_from_table(sql_statement,table_name)
print (query)
'''

# END EXAMPLE 2

########################         END UPDATE EXAMPLES           ###############################
##############################################################################################
















print("mod db ok")
