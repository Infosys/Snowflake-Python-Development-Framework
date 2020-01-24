#Copyright 2018 Infosys Ltd.
#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#http://www.apache.org/licenses/LICENSE-2.0  . 
from utilities.sf_operations import Snowflakeconnection
connection = Snowflakeconnection(profilename ='snowflake_host')
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