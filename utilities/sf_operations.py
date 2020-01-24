#Copyright 2018 Infosys Ltd.
#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#http://www.apache.org/licenses/LICENSE-2.0  . 
# This is the component which has all the snowflake execution functions
# Get a snowflake connection
# Run a query against snowflake and get the results
# Set the database, schema and warehouse for execution

import snowflake.connector as sf

from constants import constants
from utilities import sfconnection, read_conf_file, queryresult


class Snowflakeconnection:


    def __init__(self,profilename,privatekey=''):
        self.profilename = profilename
        self.privatekey = privatekey
        queryresult.clear()


    def get_snowflake_connection_withpk(self):
        """
        This function will be used to setup a connection using private key
        :return: Returns the snowflake connection mentioned in the .ini file. The calling program must pass the
        private key to make the connection
        """
        queryresult.clear()
        configparser = read_conf_file()
        userid = configparser.get(self.profilename,'userid')
        privatekey = self.privatekey
        account = configparser.get(self.profilename,'account')
        role = configparser.get(self.profilename, 'role')
        warehouse = configparser.get(self.profilename,'warehouse')
        database = configparser.get(self.profilename,'database')
        schema = configparser.get(self.profilename,'schema')

        try:
            connection = sf.connect(user=userid,private_key=privatekey,account=account)
            self.use_role(connection, role)
            self.use_database(connection,database)
            self.use_schema(connection,schema)
            self.use_warehouse(connection,warehouse)
            sfconnection['connection'] = connection
            statusmessage = "Successfully connected to database {dbname}".format(dbname=database)
            sfconnection['statusmessage'] = statusmessage
            statuscode = constants.CON000
            sfconnection['statuscode'] = statuscode
            print('--------------------------------------------------')
            print('--------------Connection Established--------------')
            print('--------------------------------------------------')
            return sfconnection
        except Exception as e:
            connresult = 'Error message is {exception}'.format(exception=e)
            sfconnection['connection'] = connresult
            statusmessage = "Failed to conect database {dbname}".format(dbname=database)
            sfconnection['statusmessage'] = statusmessage
            statuscode = constants.CON002
            sfconnection['statuscode'] = statuscode
            print('--------------------------------------------------')
            print('----------------Connection Failed-----------------')
            print('--------------------------------------------------')
            return sfconnection

    def get_snowflake_connection(self):

        """
        This function will be used to setup a connection using password. Password needs to be in conf file

        """
        queryresult.clear()
        configparser = read_conf_file()
        userid = configparser.get(self.profilename,'userid')
        password = configparser.get(self.profilename,'password')
        role = configparser.get(self.profilename,'role')
        account = configparser.get(self.profilename,'account')
        warehouse = configparser.get(self.profilename,'warehouse')
        database = configparser.get(self.profilename,'database')
        schema = configparser.get(self.profilename,'schema')


        try:
            connection = sf.connect(user=userid,password=password,account=account)
            self.use_role(connection,role)
            self.use_database(connection,database)
            self.use_schema(connection,schema)
            self.use_warehouse(connection,warehouse)

            sfconnection['connection'] = connection
            statusmessage = "Successfully connected to database: {dbname} schema: {schemaname} warehouse: {whname}".format(dbname=database,schemaname=schema,whname=warehouse)
            sfconnection['statusmessage'] = statusmessage
            statuscode = constants.CON000
            sfconnection['statuscode'] = statuscode
            print('--------------------------------------------------')
            print('--------------Connection Established--------------')
            print('Database :{}'.format(database))
            print('Role :{}'.format(role))
            print('Schema :{}'.format(schema))
            print('Warehouse :{}'.format(warehouse))
            print('--------------------------------------------------')
            return sfconnection
        except Exception as e:
            connresult = 'Error message is {exception}'.format(exception=e)
            sfconnection['connection'] = connresult
            statusmessage = "Failed to conect database {dbname}".format(dbname=database)
            sfconnection['statusmessage'] = statusmessage
            statuscode = constants.CON002
            sfconnection['statuscode'] = statuscode
            print('--------------------------------------------------')
            print('----------------Connection Failed-----------------')
            print('--------------------------------------------------')
            return sfconnection

    def use_database(self,connection,database):
        """
        This function will set the database for the session

        """
        queryresult.clear()
        try:
            snowquerystring = 'use {dbname}'.format(dbname=database)
            result = self.execute_snowquery(connection,snowquerystring)
            queryresult['result'] = result
            statusmessage = 'Query executed successfully'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE000
            queryresult['statuscode'] = statuscode
            return queryresult
        except Exception as e:
            result = 'Error message is {exception}'.format(exception=e)
            queryresult['result'] = result
            statusmessage = 'Query failed to execute'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE001
            queryresult['statuscode'] = statuscode
            return queryresult

    def use_role(self,connection,role):
        """
        This function will set the role for the session

        """
        queryresult.clear()
        try:
            snowquerystring = 'use role {rolename}'.format(rolename=role)
            result = self.execute_snowquery(connection,snowquerystring)
            queryresult['result'] = result
            statusmessage = 'Query executed successfully'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE000
            queryresult['statuscode'] = statuscode
            return queryresult
        except Exception as e:
            result = 'Error message is {exception}'.format(exception=e)
            queryresult['result'] = result
            statusmessage = 'Query failed to execute'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE001
            queryresult['statuscode'] = statuscode
            return queryresult


    def use_schema(self,connection,schema):
        """
        This function will set the schema for the session

        """
        queryresult.clear()
        try:
            snowquerystring = 'use schema {schemaname}'.format(schemaname=schema)
            result = self.execute_snowquery(connection,snowquerystring)
            queryresult['result'] = result
            statusmessage = 'Query executed successfully'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE000
            queryresult['statuscode'] = statuscode
            return queryresult

        except Exception as e:
            result = 'Error message is {exception}'.format(exception=e)
            queryresult['result'] = result
            statusmessage = 'Query failed to execute'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE001
            queryresult['statuscode'] = statuscode
            return queryresult

    def use_warehouse(self,connection,warehouse):
        """
        This function will set the warehouse for the session

        """
        queryresult.clear()
        try:
            snowquerystring = 'use warehouse {whname}'.format(whname=warehouse)
            result = self.execute_snowquery(connection,snowquerystring)
            queryresult['result'] = result
            statusmessage = 'Query executed successfully'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE000
            queryresult['statuscode'] = statuscode
            return queryresult
        except Exception as e:
            result = 'Error message is {exception}'.format(exception=e)
            queryresult['result'] = result
            statusmessage = 'Query failed to execute'
            queryresult['statusmessage'] = statusmessage
            statuscode = constants.EXE001
            queryresult['statuscode'] = statuscode
            return queryresult

    def execute_snowquery(self,connection,snowquerystring,asyncflag=False):
        """
        This function takes the query as input and outputs the result of query. If the asyncflag is true
        the function will submit the query to snowflake and will immediately return without waiting for the
        results of the query. It will return the query id which can be tracked to find out the completion of the
        query. The async method can be used in case of large queries that may take a longer time to execute
        :param connection: The sonowflake connection that will be used to execute the query
        :param snowquerystring: The actual query to be executed
        :param asyncflag: True - query will be submitted in asynchronous mode, False is default
        :return: Return the queryresult dictionary
        """


        queryresult.clear()

        if asyncflag:
            try:
                print('-------------Asynchronous call to snowflake-------------')
                cursor = connection.cursor()
                result= cursor.execute(snowquerystring,_no_results=True)
                queryid = cursor.sfqid
                queryresult['queryid'] = queryid
                queryresult['result'] = result
                statusmessage = 'Query submitted to snowflake'
                queryresult['statusmessage']=statusmessage
                statuscode = constants.EXE000
                queryresult['statuscode']=statuscode
                return queryresult
            except Exception as e:
                result = 'Error message is {exception}'.format(exception=e)
                queryresult['result'] = result
                statusmessage = 'Query failed to execute'
                queryresult['statusmessage'] = statusmessage
                statuscode = constants.EXE001
                queryresult['statuscode'] = statuscode
                return queryresult
            finally:
                if connection is None:
                    pass
                else:
                    cursor.close()


        try:
            cursor = connection.cursor()
            cursor.execute(snowquerystring)
            result = cursor.fetchall()
            queryresult['queryid'] = cursor.sfqid
            queryresult['result'] = result
            statusmessage = 'Query executed successfully'
            queryresult['statusmessage']=statusmessage
            statuscode = constants.EXE000
            queryresult['statuscode']=statuscode
            return queryresult
        except Exception as e:
            result = 'Error message is {exception}'.format(exception=e)
            queryresult['queryid'] = cursor.sfqid
            queryresult['result'] = result
            statusmessage = 'Query failed to execute'
            queryresult['statusmessage']=statusmessage
            statuscode = constants.EXE001
            queryresult['statuscode']=statuscode
            return queryresult
        finally:
            if connection is None:
                pass
            else:
                cursor.close()

    def execute_stream(self,connection,queryfile):
        """
        This function will take a script file as input and will execute all the queries in the file
        one by one. The calling program must retrieve the result from the queryresult dictionary
        and loop through the result to get the output of each query
        :param connection: The connection that will be used to execute the script
        :param queryfile: The text file which has all the queries that will be executed
        :return: Returns queryresult dictionary
        """
        queryresult.clear()
        try:
            filename = open(queryfile,'r',encoding='utf-8')
            result = connection.execute_stream(filename)
            queryresult['queryid'] = ''
            queryresult['result'] = result
            statusmessage = 'Script successfully executed'
            queryresult['statusmessage']=statusmessage
            statuscode = constants.EXE000
            queryresult['statuscode']=statuscode
            return queryresult
        except Exception as e:
            result = 'Error message is {exception}'.format(exception=e)
            queryresult['queryid'] = ''
            queryresult['result'] = result
            statusmessage = 'Script failed to execute'
            queryresult['statusmessage']=statusmessage
            statuscode = constants.EXE001
            queryresult['statuscode']=statuscode
            return queryresult
