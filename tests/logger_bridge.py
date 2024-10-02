#!/usr/bin/env python3
from time import sleep
from bayeosgatewayclient import BayEOSWriter, BayEOSSender
import tempfile
from os import path
from campbell.logger import Logger
from pytz import timezone
import pytz 
from datetime import datetime
import logging
import socket 

# Please adapt the following variables
GATEWAY_URL = 'https://<hostname>/gateway/frame/saveFlat'
LOGGER_URL = "http://<hostname>"
LOGGER_TABLE = "stats_table"
LOGGER_TIMEZONE = 'Etc/GMT-1'
LOGGER_NAME = socket.gethostname()
LOGGER_NAN = "NAN"
MOST_RECENT = 1000
SLEEP_SECS = 600
GATEWAY_USER = "<user>"
GATEWAY_PASSWORD = "<password>"

# Logging
log = logging.getLogger('root')

# Gateway Client
path = path.join("/data","{}-data".format(LOGGER_NAME)) 
writer = BayEOSWriter(path)
writer.save_msg('Writer started.')
sender = BayEOSSender(path, "{}/{}".format(LOGGER_NAME,LOGGER_TABLE), GATEWAY_URL,GATEWAY_PASSWORD,GATEWAY_USER)
sender.start()

# Cambell logger 
logger = Logger(LOGGER_URL)
lastRec = None
timeZone = timezone(LOGGER_TIMEZONE)

while True:
    try:
        if lastRec is None:
            log.info("Initial import")
            data = logger.dataMostRecent("dl:{}".format(LOGGER_TABLE),MOST_RECENT)
        else:
            log.info("Delta import")
            data = logger.dataSinceRecord("dl:{}".format(LOGGER_TABLE),lastRec)                    
        n = 0
        for rec in data['data']: 
            values = {}            
            for i, field in enumerate(data['head']['fields']):               
                if field['type'] == 'xsd:float':
                    v = rec['vals'][i]
                    if v!=LOGGER_NAN and field['type'] == 'xsd:float':
                        values[field['name']] = float(v)                        
            if len(values)>0:
                dt = timeZone.localize(datetime.strptime(rec['time'],'%Y-%m-%dT%H:%M:%S'))
                writer.save(values=values,value_type=0x61,timestamp=dt.timestamp()) 
            lastRec= rec['no']
            n = n + 1
        log.info("{} records fetched. Last record:{}".format(n, lastRec))
        writer.flush()
    except Exception as e:
        log.error("Request failed:{}".format(e))      
    log.info("Going to sleep")
    sleep(SLEEP_SECS)
