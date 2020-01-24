#Copyright 2018 Infosys Ltd.
#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#http://www.apache.org/licenses/LICENSE-2.0  . 
from configparser import ConfigParser
from pathlib import Path

sfconnection={}
connection=''
statusmessage=''
statuscode=''
queryresult={}

def read_conf_file():
    configuration_path=Path(__file__).parent / "../connections/conf.ini"
    configparser = ConfigParser()
    configparser.read(configuration_path)
    return configparser
