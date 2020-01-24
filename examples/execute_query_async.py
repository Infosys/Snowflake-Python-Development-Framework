#Copyright 2018 Infosys Ltd.
#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#http://www.apache.org/licenses/LICENSE-2.0  . 
from utilities.sf_operations import Snowflakeconnection
connection = Snowflakeconnection(profilename ='snowflake_host')
sfconnectionresults = connection.get_snowflake_connection()

sfconnection = sfconnectionresults.get('connection')
statuscode = sfconnectionresults.get('statuscode')
statusmessage = sfconnectionresults.get('statusmessage')


querystring = "select * from ADMCOE_SALES;"
queryresult = connection.execute_snowquery(sfconnection,querystring,asyncflag=True)

queryid = queryresult.get('queryid')
executionresult = queryresult.get('result')
statuscode = queryresult.get('statuscode')
statusmessage = queryresult.get('statusmessage')

print (queryid,statuscode,statusmessage)

sfconnection.close()