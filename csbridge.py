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
    writer = BayEOSWriter(pa,max_chunk = conf.getint(SEC_BRIDGE, 'max_chunk_size'))              
    sender = BayEOSSender(pa,conf.get(SEC_BRIDGE,'name'), conf.get(SEC_GATEWAY,'url') + '/frame/saveFlat', conf.get(SEC_GATEWAY,'password'), conf.get(SEC_GATEWAY,'username'))
    sender.start()
    
    gw_url = conf.get(SEC_GATEWAY,'url')
    gw_username = conf.get(SEC_GATEWAY,'username')
    gw_password = conf.get(SEC_GATEWAY,'password')


    sleep_time = conf.getint(SEC_BRIDGE,'sleep_time')

    loggers = getLoggerList(conf)                              
    lrt_map = getLrtMap(gw_url,gw_username,gw_password,loggers)     
    
    while True:  
        for logger in loggers:                                  
                host, tz, tables = logger                                                                          
                for table in tables.split(','):                   
                    origin = host + '/' + table                         
                    try:                                                      
                        if origin in lrt_map:                                                       
                            lrt = lrt_map[origin]
                            if lrt is None:
                                log.info("Initial import of {0}".format(origin))
                                since = tz_utc.localize(datetime.strptime(conf.get(SEC_BRIDGE,'initial_since'),ISO_TIMESTAMP))                                                                                                                                                      
                            else:
                                log.info("Delta import of {0}.".format(origin))                             
                                since = lrt + timedelta(microseconds=+1)                                                            
                            
                            f_date = fetchAndSaveData(writer,host,table,timezone(tz),since,origin)
                            if f_date is not None:
                                lrt_map[origin] = f_date
                        else:                            
                            log.warn("Try to get last result time of {0} from gateway.".format(origin))
                            lrt_map[origin] = getLastResultTime(gw_url, gw_username, gw_password, origin) 

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

def getLrtMap(url,username,password,loggers):   
    """
    @param url: gateway url 
    @param username: gateway user name 
    @param password: gateway user password
    @param loggers: list of logger tuples      
    """  
    
    log.info("Polling gateway for last result time values ...")
    lrt_map = {}    
    for logger in loggers:
        host, tz, tables = logger
        for table in tables.split(','):
            origin = host + '/' + table
            try:
                lrt_map[origin] = getLastResultTime(url,username,password,origin)
            except:
                log.warning('Failed to get last result time of {0} from gateway.'.format(origin) )
    return lrt_map                        
    
def getLoggerList(conf):
    loggers = []
    for section in conf.sections():
        if section.startswith('logger'):
            loggers.append((conf.get(section,'host'),conf.get(section,'tz'),conf.get(section,'tables')))
    return loggers           
    
def fetchAndSaveData(writer,host,table,tz,since,origin):
    """ Fetch and save records from Campbell logger 
    @param host: logger hostname
    @param table: logger table name
    @param tz: time zone of logger datetime values 
    @param since: datetime
    @returns lrt: as datetime
    """    
    
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
                if row[index+1] != '':
                    values[cha] = float(row[index+1])                                                                                          
            writer.save(values,value_type=0x61,timestamp=timestamp,origin=origin)
            lrt = dt            
            rc = rc + 1;
    # fwrite.flush()
    log.info("{0} rows saved.".format(str(rc)))
    if lrt is not None:
        return lrt.astimezone(pytz.utc)
    else:
        return None
    
    

def getLastResultTime(url,username,password,origin):
    """
    Get the last result time value for channel on a BayEOS Gateway 
    @param url: gateway url
    @param username: gateway user name 
    @param password: gateway password
    @param origin: board origin   
    @return last result time as datetime             
    """               
    request = urllib2.Request(url + "/board/findByOrigin?origin={1}".format(url,urllib.quote_plus(origin)))
    base64string = base64.standard_b64encode('{0}:{1}'.format(username, password))
    request.add_header("Authorization", "Basic {0}".format(base64string))
    try:   
        result = urllib2.urlopen(request)
        t = json.load(result)['lastResultTime'] 
        if t is None:            
            return None
        else:                        
            return pytz.utc.localize(datetime.strptime(t[:-1],'%Y-%m-%dT%H:%M:%S')) 
    except urllib2.HTTPError as err:
        if err.code == 404:
            log.warning("Origin:{0} not found.".format(origin))            
            return None
        else:
            log.error(str(err))
            raise err
            
    
            

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
    if not conf.has_option(SEC_BRIDGE, 'path'):
        conf.set(SEC_BRIDGE, 'path', os.path.join(tempfile.gettempdir(),conf.get(SEC_BRIDGE,'name')))    
    if not conf.has_option(SEC_BRIDGE, 'max_chunk_size'):
        conf.set(SEC_BRIDGE, 'max_chunk_size', "32000")
    if not conf.has_option(SEC_BRIDGE, 'initial_since'):
        conf.set(SEC_BRIDGE, 'initial_since', datetime.strftime(datetime.utcnow(),ISO_TIMESTAMP))
    return conf
        
if __name__ == '__main__':
    main()