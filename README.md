# Snowflake-Python-Development-Framework
snowflake sdk for python

This package has been built to help developers build applications using snowflake quickly. Below listed are some examples on how to work with this package

# Create a snowflake connection
You can create the connection using either a private key or password. The connection details have to be upated in the conf.ini file

Sample code
--------------

Connecting to snowflake
------------------------

The below piece of code connects to snowflake and returns the connection object, statuscode and statusmessage

```python

from utilities.sf_operations import snowflakeconnection

connection = snowflakeconnection(profilename ='snowflake_host')
sfconnectionresults = connection.get_snowflake_connection()

sfconnection = sfconnectionresults.get('connection')
statuscode = sfconnectionresults.get('statuscode')
statusmessage = sfconnectionresults.get('statusmessage')

print(sfconnection,statuscode,statusmessage)

```
Execute a query in snowflake:
-----------------------------------
The below piece of code executes a query in snowflake

```python
from utilities.sf_operations import snowflakeconnection
connection = snowflakeconnection(profilename ='snowflake_host')
sfconnectionresults = connection.get_snowflake_connection()

sfconnection = sfconnectionresults.get('connection')
statuscode = sfconnectionresults.get('statuscode')
statusmessage = sfconnectionresults.get('statusmessage')

querystring = "select * from sales;"
queryresult = connection.execute_snowquery(sfconnection,querystring)

queryid = queryresult.get('queryid') #This is the query id in SF
executionresult = queryresult.get('result')
statuscode = queryresult.get('statuscode')
statusmessage = queryresult.get('statusmessage')

print (queryid,statuscode,statusmessage)
for results in executionresult:
    print(results)

sfconnection.close()

```
Execute a query in snowflake in asynchrouos mode:
------------------------------------------------
This uses the same function but with asyncflag as true

```python
from utilities.sf_operations import snowflakeconnection
connection = snowflakeconnection(profilename ='snowflake_host')
sfconnectionresults = connection.get_snowflake_connection()

sfconnection = sfconnectionresults.get('connection')
statuscode = sfconnectionresults.get('statuscode')
statusmessage = sfconnectionresults.get('statusmessage')

#print(sfconnection,statuscode,statusmessage)

querystring = "select * from ADMCOE_SALES;"
queryresult = connection.execute_snowquery(sfconnection,querystring,asyncflag=True)

queryid = queryresult.get('queryid')
executionresult = queryresult.get('result')
statuscode = queryresult.get('statuscode')
statusmessage = queryresult.get('statusmessage')

```
Execute a snowflake script in snowflake:
------------------------------------------------
This feature can be used to execute a script file with one or
more snowflake queries

```python
from utilities.sf_operations import snowflakeconnection
connection = snowflakeconnection(profilename ='snowflake_host')
sfconnectionresults = connection.get_snowflake_connection()

sfconnection = sfconnectionresults.get('connection')
statuscode = sfconnectionresults.get('statuscode')
statusmessage = sfconnectionresults.get('statusmessage')

#print(sfconnection,statuscode,statusmessage)

filename = "D://script.sql"
queryresult = connection.execute_stream(sfconnection,filename)

executionresult = queryresult.get('result')
statuscode = queryresult.get('statuscode')
statusmessage = queryresult.get('statusmessage')

for cursor in executionresult:
    for ret in cursor:
        print(ret)

print (executionresult,statuscode,statusmessage)

sfconnection.close()
```

Below features are currently in development
------------------------------------------------------
1. Put file to a stage after splitting into multiple files
2. Compress files before putting them to stage
3. Archive files after putting them to stage






