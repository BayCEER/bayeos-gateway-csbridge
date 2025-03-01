#!/usr/bin/python3
'''
Campbell Scientific Bridge for BayEOS 
- Reads observation data from logger using http interface  
- Saves data in BayEOS labeled frame format by bayeosgatewayclient 
@author: oliver.archner@uni-bayreuth.de
Created on 06.07.2022
'''

import socket
from time import sleep
from bayeosgatewayclient import BayEOSWriter, BayEOSSender
from configparser import ConfigParser
import tempfile
import csv
from datetime import datetime, timedelta
import os
import sys
import logging
from campbell.logger import Logger
from zoneinfo import ZoneInfo


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=logging.INFO) 

SEC_BRIDGE = 'csbridge'
SEC_GATEWAY = 'gateway'
LOGGER_NAN = "NAN"


def main():
    conf = readConfig()        
    pa = conf.get(SEC_BRIDGE,'path')           
    writer = BayEOSWriter(pa, max_chunk=10000)              
    sender = BayEOSSender(pa,'csbridge', conf.get(SEC_GATEWAY,'url'), conf.get(SEC_GATEWAY,'password'), conf.get(SEC_GATEWAY,'username'))
    sender.start()
    
    sleep_time = conf.getint(SEC_BRIDGE,'sleep_time')

    lr_map = {}

    while True:  
        for logger in getLoggerList(conf):                                  
                host, tz, tables = logger                                                                          
                for table in tables.split(','):                   
                    origin = host + '/' + table                         
                    try:
                        lrec = None
                        if origin in lr_map:
                            lrec = lr_map[origin]
                        lr_map[origin] = fetchAndSaveData(writer,host,table,tz,lrec,origin,conf)                        
                    except Exception as err:
                        logging.error("Communication error:{0}".format(str(err)))                                            
                    else: 
                        logging.info("Import of {0} finished.".format(origin))        
        logging.info("Going to sleep for {0} secs ...".format(sleep_time))
        sleep(sleep_time)

def getLoggerList(conf):
    loggers = []
    for section in conf.sections():
        if section.startswith('logger'):
            loggers.append((conf.get(section,'host'),conf.get(section,'tz'),conf.get(section,'tables')))
    return loggers           
    
def fetchAndSaveData(writer,host,table,tz,lastRec,origin,conf, re=0):
    """ Fetch and save records from Campbell logger 
    @param host: logger hostname
    @param table: logger table name
    @param tz: time zone of logger datetime values as text
    @param lastRec: last record id as integer
    @returns lastRec: as integer
    """   
    logging.info("Fetch data from {}".format(origin))
    logger = Logger('http://'+ host)     
    if lastRec is None:        
        logging.info("Get most recent data")
        data = logger.dataMostRecent("dl:{}".format(table),conf.getint(SEC_BRIDGE,'most_recent'))        
    else:        
        logging.info("Get data since record:{}".format(lastRec))
        data = logger.dataSinceRecord("dl:{}".format(table),lastRec)   
    n = 0
    for rec in data['data']: 
            values = {}            
            for i, field in enumerate(data['head']['fields']):               
                if field['type'][4:] in ['boolean','decimal','float','double','byte','int','integer','long','negativeInteger','nonNegativeInteger','nonPositiveInteger','positiveInteger',
'short','unsignedByte','unsignedInt','unsignedLong','unsignedShort']:
                    v = rec['vals'][i]
                    if v!=LOGGER_NAN:
                        values[field['name']] = float(v)                        
            if len(values)>0:
                dt = datetime.strptime(rec['time'],'%Y-%m-%dT%H:%M:%S')
                dt = dt.replace(tzinfo=ZoneInfo(tz))
                writer.save(values=values,value_type=0x61,timestamp=dt.timestamp(),origin=origin) 
            lastRec= rec['no']
            lastTime = rec['time']
            n = n + 1
    logging.info("{} records fetched. Last record:{} Last time:{}".format(n, lastRec, lastTime))
    if 'more' in data and data['more'] and re < 10:
        sleep(5) 
        logging.info("Attempt {} to get more data".format(re+1))
        re = re + 1
        lastRec = fetchAndSaveData(writer,host,table,tz,lastRec,origin,conf,re)
    writer.flush()    
    return lastRec
    
    
def readConfig():
    """
        Reads the config file and populates default values 
        @return ConfigParser
    """
    conf = ConfigParser()
    if sys.platform == 'win32':        
        conf.read('csbridge.conf')
    else:
        conf.read('/etc/csbridge.conf')
    
    # Overwrite default values
    if not conf.has_option(SEC_BRIDGE, 'name'):
        conf.set(SEC_BRIDGE,'name',socket.gethostname())    
    if not conf.has_option(SEC_BRIDGE, 'path'):
        conf.set(SEC_BRIDGE, 'path', os.path.join(tempfile.gettempdir(),conf.get(SEC_BRIDGE,'name')))    
    if not conf.has_option(SEC_BRIDGE, 'most_recent'):
        conf.set(SEC_BRIDGE, 'most_recent',str(50000))     
    if not conf.has_option(SEC_BRIDGE, 'sleep_time'):
        conf.set(SEC_BRIDGE, 'sleep_time',str(600))         
    return conf
        
if __name__ == '__main__':
    main()
