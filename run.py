#!/usr/bin/env python
# -*- coding: utf-8 -*-

# system and utils
import sys, time, re, json
from multiprocessing import Process, JoinableQueue
import logging

# data sources
import pymongo
import sunburnt
import jsonrpclib

# logging
import TimeElapsedLogging


def indexer(web_page_pile, solr):

    while True:
        todo = []
        while not pile.empty():
            todo.append(pile.get())
        save = prepare_tweets(todo)
        for t in save:
            tid = db.save(t)
    #        sys.stderr.write("DEBUG: saved tweet %s\n" % tid)


def mongo_retriever(web_entity_pile, web_page_pile,mongo_coll,accepted_content_types):
    log=TimeElapsedLogging.create_log("mongo_retriever","mongo_retriever.log")
    while True:
        todo = []
        while not web_entity_pile.empty():
            we=web_entity_pile.get()
            log.log(logging.INFO,"getting pages from %s"%we["name"])
            web_pages=we["web_pages"]
            mongo_coll.find({"url": {"$in": [page["url"] for page in web_pages]},
               "content_type": {"$in": accepted_content_types}
             })
            web_entity_pile.task_done()



def hyphe_core_retriever(web_entity_pile,hyphe_core,web_entity_status):
    log=TimeElapsedLogging.create_log("hyphe_core_retriever","hyphe_core_retriever.log")
    web_entities = hyphe_core.store.get_webentities_by_status(web_entity_status)
    nb_web_entities=len(web_entities["result"])
    web_entities=web_entities["result"]
    for we in web_entities: 
        web_pages = hyphe_core.store.get_webentity_pages(we["id"])
        log.log(logging.INFO,"retrieved %s pages of web entity %s"%(len(web_pages["result"]),we["name"]))
        # total_pages+=len(web_pages["result"])
        we["web_pages"]=web_pages
        web_entity_pile.put(we)


def pile_logger(web_entity_pile,web_page_pile):
    while True :
        print "%s items in web_entity_pile, %s items in web_page_pile"%(web_entity_pile.qsize(),web_page_pile.qsize())
        time.sleep(0.5)

if __name__=='__main__':
    try:
        with open('config.json') as confile:
            conf = json.loads(confile.read())
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not read configuration\n')
        sys.exit(1)
    try:
        db = pymongo.Connection(conf['mongo']['host'], conf['mongo']['port'])[conf["mongo"]["db"]]
        coll = db[conf['mongo']['web_pages_collection']]
        coll.ensure_index([('url', pymongo.ASCENDING)], background=True)
        # prepare conte_type filter
        with open(conf['mongo']['contenttype_whitelist_filename']) as content_type_whitelist :
            accepted_content_types=content_type_whitelist.read().split("\n")
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not initiate connection to MongoDB\n')
        sys.exit(1)
    # solr
    try:
        solr = sunburnt.SolrInterface("http://%s:%s/%s" % (conf["solr"]['host'], conf["solr"]['port'], conf["solr"]['path'].lstrip('/')))
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not initiate connection to SOLR node\n')
        sys.exit(1)
    # hyphe core
    try:
        hyphe_core=jsonrpclib.Server('http://%s:%s'%(conf["hyphe-core"]["host"],conf["hyphe-core"]["port"]))
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not initiate connection to hyphe core\n')
        sys.exit(1)


    web_entity_pile = JoinableQueue()
    web_page_pile = JoinableQueue()    

    pile_logger_proc = Process(target=pile_logger,args=((web_entity_pile),(web_page_pile)))
    pile_logger_proc.daemon = True
    pile_logger_proc.start()

    hyphe_core_proc = Process(target=hyphe_core_retriever, args=((web_entity_pile), hyphe_core,"IN"))
    hyphe_core_proc.daemon = True
    hyphe_core_proc.start()

    mongo_proc = Process(target=mongo_retriever, args=((web_entity_pile), (web_page_pile), coll, accepted_content_types))
    mongo_proc.daemon = True
    mongo_proc.start()

    solr_proc = Process(target=indexer, args=((web_page_pile), solr))
    solr_proc.daemon = True
    #solr_proc.start()

    # wait the first provider to finish
    hyphe_core_proc.join()
    # wait the pile to be processed
    web_entity_pile.join()