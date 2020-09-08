#!/usr/bin/python -u
'''
Campbell Scientific Bridge for BayEOS 
- Reads observation data in toa5 format from logger using http interface  
- Saves data in BayEOS labeled frame format by bayeosgatewayclient 
@author: oliver.archner@uni-bayreuth.de
Created on 19.05.2016
'''

from time import sleep
from bayeosgatewayclient import BayEOSWriter, BayEOSSender
from ConfigParser import SafeConfigParser
import tempfile
import urllib2
import urllib
import base64
import json
import csv
from datetime import datetime, timedelta
import os
import sys
from pytz import timezone
import pytz
import logging
import socket

log = logging.getLogger('root')

SEC_BRIDGE = 'csbridge'
SEC_GATEWAY = 'gateway'
ISO_TIMESTAMP = '%Y-%m-%dT%H:%M:%S'
CSV_TIMESTAMP = '%Y-%m-%d %H:%M:%S'


def main():
    conf = readConfig()    
    log.setLevel(str(conf.get(SEC_BRIDGE,'log_level')).upper())       
    tz_utc = pytz.utc    

    pa = conf.get(SEC_BRIDGE,'path')           
    writer = BayEOSWriter(pa, max_chunk=20000)              
    sender = BayEOSSender(pa,conf.get(SEC_BRIDGE,'name'), conf.get(SEC_GATEWAY,'url') + '/frame/saveFlat', conf.get(SEC_GATEWAY,'password'), conf.get(SEC_GATEWAY,'username'))
    sender.start()
    
    gw_url = conf.get(SEC_GATEWAY,'url')
    gw_username = conf.get(SEC_GATEWAY,'username')
    gw_password = conf.get(SEC_GATEWAY,'password')

    sleep_time = conf.getint(SEC_BRIDGE,'sleep_time')

    lrt_map = {}

    while True:  
        for logger in getLoggerList(conf):                                  
                host, tz, tables = logger                                                                          
                for table in tables.split(','):                   
                    origin = host + '/' + table                         
                    try:                                                      
                        if origin in lrt_map:                                                       
                            lrt = lrt_map[origin]
                            log.info("Delta import of {0}.".format(origin))                             
                            since = lrt + timedelta(microseconds=+1)                                                                                               
                        else:                            
                            log.info("Initial import of {0}".format(origin))
                            since = None
                        f_date = fetchAndSaveData(writer,host,table,timezone(tz),since,origin,conf)
                        if f_date is not None:
                            lrt_map[origin] = f_date
                    except urllib2.HTTPError as err:
                        log.error("HTTP communication error:{0}".format(str(err)))                        
                    except urllib2.URLError as err:
                        log.error('URLError: ' + str(err))
                    except ValueError as err:
                        log.error("Value error:{0}".format(str(err)))
                    except:
                        log.error('Unspecified error.' )
                    else: 
                        log.info("Import of {0} finished.".format(origin))
        
        log.info("Going to sleep for {0} secs ...".format(sleep_time))
        sleep(sleep_time)

def getLoggerList(conf):
    loggers = []
    for section in conf.sections():
        if section.startswith('logger'):
            loggers.append((conf.get(section,'host'),conf.get(section,'tz'),conf.get(section,'tables')))
    return loggers           
    
def fetchAndSaveData(writer,host,table,tz,since,origin,conf):
    """ Fetch and save records from Campbell logger 
    @param host: logger hostname
    @param table: logger table name
    @param tz: time zone of logger datetime values 
    @param since: datetime
    @returns lrt: as datetime
    """    

    if since is None:
        url = 'http://{0}/?command=dataquery&uri=dl:{1}&format=toa5&mode=most-recent&p1={2}'.format(host,table,conf.getint(SEC_BRIDGE,'most_recent'))         
        log.debug("Query host:{0} table:{1}".format(host,table))
    else:
        since_str = datetime.strftime(since.astimezone(tz),'%Y-%m-%dT%H:%M:%S.%f')    
        log.debug("Query host:{0} table:{1} since:{2}".format(host,table,since_str))
        url = 'http://{0}/?command=dataquery&uri=dl:{1}&format=toa5&mode=since-time&p1={2}'.format(host,table,since_str)       
    request = urllib2.Request(url)
    res = urllib2.urlopen(request,timeout=30)
    reader = csv.reader(res)
    r = 0
    rc = 0
    key = []
    lrt = None
         
    for row in reader:
        r = r + 1
        if r == 2:
            key = row                                                                    
        elif r > 4:                                                         
            dt = tz.localize(datetime.strptime(row[0],CSV_TIMESTAMP))     
            timestamp=(dt-datetime(1970,1,1,tzinfo=pytz.utc)).total_seconds()                                                       
            values = {}   
            for index, cha in enumerate(key[1:]):
                if row[index+1] == '':
                    log.warning("Empty value for channel {0} at {1}.".format(cha,row[0]))
                elif row[index+1] == 'NAN':
                    log.warning("NAN value for channel {0} at {1}.".format(cha,row[0]))
                else: 
                    values[cha] = float(row[index+1])
            log.debug("Ts:{0} Values:{1}".format(dt,values))
            writer.save(values,value_type=0x61,timestamp=timestamp,origin=origin)
            lrt = dt            
            rc = rc + 1;
    writer.flush()
    log.info("{0} rows saved.".format(str(rc)))
    sleep(5);
    if lrt is not None:
        return lrt.astimezone(pytz.utc)
    else:
        return None
    
    
def readConfig():
    """
        Reads the config file and populates default values 
        @return SafeConfigParser
    """
    conf = SafeConfigParser()
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
        conf.set(SEC_BRIDGE, 'most_recent',str(2000))    

   
    return conf
        
if __name__ == '__main__':
    main()
