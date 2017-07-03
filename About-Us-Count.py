#!/usr/bin/python
# this script for counting index from elasticsearch to show in about us page.
import os
import sys, socket, io, json, getopt, time
import requests, logging
from datetime import datetime
from elasticsearch import Elasticsearch
from subprocess import *


GTM_APPS_ES_VERSION = '1.0'

def info_main():
   print "Usage: %s [OPTIONS]" % os.path.basename(sys.argv[0])
   print "example: %s -c config.json -a localhost -p 9200 -b count" % os.path.basename(sys.argv[0])
   print ""
   print('Where OPTIONS:')
   print('-c FILE       Specify path of config file')
   print('-a HOST       Specify URL address of the Elasticsearch server')
   print('-p PORT       Specify Port of the Elasticsearch server')
   print('-h            Printing the help')
   print('-b            To do job ex:count => for counting')
   print('-v            Print the version')
   print ""

def es_write_log(loglevel,logmsg):
    FORMAT = '%(asctime)s - %(name)s - [%(process)d] - %(levelname)s - %(message)s'
    logging.basicConfig(filename='AboutUs-count.log',format=FORMAT)
    logger = logging.getLogger(socket.gethostname())
    logger.setLevel(logging.DEBUG)
    if loglevel == "debug":
        logger.debug(logmsg)
    elif loglevel == "info":
        logger.info(logmsg)
    elif loglevel == "warn":
        logger.warn(logmsg, exc_info=True)
    elif loglevel == "error":
        logger.error(logmsg,exc_info=True)
    elif loglevel == "critical":
        logger.critical(logmsg, exc_info=True)

def do_count(host, port, fileConfig):
    try:
        es_write_log("info", "Service counting index now running ...")

        configMain = open(fileConfig)
        rdMain = json.loads(configMain.read())
        createFileJson = rdMain['dirOutput']+"/"+rdMain['fileOutput']
        es = Elasticsearch([{'host': host, 'port': port, 'timeout': 1000}])

        # ignore 404 cause by IndexAlreadyExistsException when index missing
        # es_write_log("info", "grab index for counter ...")
        counter = {}
        User = es.count(index=rdMain['indexEsUser'],ignore=[404])
        PageView = es.count(index=rdMain['indexEsPageView'],ignore=[404])
        Store = es.count(index=rdMain['indexEsStore'],ignore=[404])
        merchant = es.count(index=rdMain['indexEsMerchant'],ignore=[404])
        counter.update({"users":User['count'],"page_views":PageView['count'],"stores":Store['count'],"merchants":merchant['count']})

        #Encode JSON data
        with open(createFileJson, 'w') as f:
             json.dump(counter, f)

    except Exception as e:
        es_write_log("error",e)
        sys.exit(e)

def main(argv):
   if len(sys.argv) == 1:
       info_main()
   try:
      opts, args = getopt.getopt(argv,"c:a:p:b:hv")
   except getopt.GetoptError as err:
       es_write_log("error",err)
       info_main()
       sys.exit(2)
   for opt, arg in opts:
        if opt == "-v":
            print 'Version:', GTM_APPS_ES_VERSION
        elif opt == "-c":
            GTM_APPS_ES_CONF = arg
            SG_DO_ES_CONF = True
        elif opt == "-a":
            GTM_APPS_ES_HOST = arg
            SG_DO_ES_HOST = True
        elif opt == "-p":
            GTM_APPS_ES_PORT = arg
            SG_DO_ES_PORT = True
        elif opt == "-b":
            GTM_APPS_ES_TODO = arg
            try:
                if SG_DO_ES_CONF|SG_DO_ES_HOST|SG_DO_ES_PORT == True:
                    if GTM_APPS_ES_TODO == "count":
                        do_count(GTM_APPS_ES_HOST,GTM_APPS_ES_PORT,GTM_APPS_ES_CONF)
                    else:
                        es_write_log("info", "Nothing todo please use option todo ...")
                        info_main()
                        sys.exit()

            except Exception as e:
                es_write_log("error",e)
                sys.exit(e)
        elif opt in ("-h"):
            info_main()
            sys.exit()

if __name__ == "__main__":
   main(sys.argv[1:])
