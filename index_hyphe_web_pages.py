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
    log=TimeElapsedLogging.create_log("solr_indexer","solr_indexer.log")
    
    log.log(logging.INFO,"started")

    while True: 
        try :
            web_entity=web_page_pile.get()
            log.log(logging.INFO,"sarting %s"%web_entity["name"])
            nb_pages=0
            for page_mongo in web_entity["pages_mongo"]:
                if "body" in page_mongo.keys():
                    
                    body = page_mongo["body"].decode('zip')
                    try:
                        body = body.decode(page_mongo.get("encoding",""))
                        encoding = page_mongo.get("encoding","")
                    except Exception :
                        body = body.decode("UTF8","replace")
                        encoding = "UTF8-replace"
                    solr_document={
                        "id":page_mongo["_id"],
                        "web_entity":web_entity["name"],
                        "web_entity_id":web_entity["id"],
                        "corpus":"hyphe",
                        "encoding":encoding,
                        "original_encoding":page_mongo.get("encoding",""),
                        "url":page_mongo["url"],
                        "lru":page_mongo["lru"],
                        "depth":page_mongo["depth"],
                        "content":body
                    }
                    
                    #solr_json_docs.append(solr_document)
                    try:
                         solr.add(solr_document)
                         nb_pages+=1
                    except Exception :
                        log.info("Exception with document :%s %s %s"%(solr_document["id"],solr_document["url"],solr_document["encoding"]))
                        #TODO : write error document to disk
            
            log.log(logging.INFO,"'%s' indexed (%s web pages on %s)"%(web_entity["name"],nb_pages,len(web_entity["pages_mongo"])))
        except Exception as e:
            log.exception("exception in indexer")#"%s %s"%(type(e),e))
        web_page_pile.task_done()	    


def mongo_retriever(web_entity_pile, web_page_pile,coll,accepted_content_types):
    
        log=TimeElapsedLogging.create_log("mongo_retriever",filename="mongo_retriever.log")
        while True:
            try:
                we=web_entity_pile.get()
                urls=[page["url"] for page in we["web_pages"]]
                nb_urls=len(urls)
                last_id=""
                pages_mongo=[]


                i=0
                url_slice_len=1000
                while i<len(urls) :
                    urls_slice=urls[i:i+url_slice_len]
                    pages_mongo_slice=coll.find({
                            "url": {"$in": urls_slice},
                            "content_type": {"$in": accepted_content_types}
                        },
                        fields=["_id","encoding","url","lru","depth","body"])
                    log.debug("%s %s: got %s pages in slice %s %s"%(we["name"],we["id"],pages_mongo_slice.count(),i,len(urls_slice)))
                    pages_mongo+=list(pages_mongo_slice)
                    i=i+url_slice_len

                # while True :
                #     pages_mongo_slice=coll.find({
                #             "url": {"$in": urls},
                #             "content_type": {"$in": accepted_content_types},
                #             "_id": {"$gt": last_id}}
                #         },
                #         fields=["_id","encoding","url","lru","depth","body"]).limit(1000).sort("_id")
                #     if pages_mongo_slice.count(with_limit_and_skip=True)>0 :
                #         pages_mongo.append(list(pages_mongo_slice))
                #         last_id=pages_mongo[-1]["_id"]
                #     else:
                #         # no more pages to retrieve
                #         break
                if len(pages_mongo)>0:
                    del(we["web_pages"])
                    we["pages_mongo"]=pages_mongo
                    web_page_pile.put(we)
                    log.log(logging.INFO,"got %s mongo pages on %s web page urls from %s"%(len(pages_mongo),nb_urls,we["name"]))
                else:
                    log.warning("no pages on %s retrieved from %s"%(nb_urls,we["name"]))
            except Exception as e : 
                log.log(logging.ERROR,"%s %s"%(type(e),e))
            web_entity_pile.task_done()



def hyphe_core_retriever(web_entity_pile,web_page_pile,hyphe_core_url):
    log=TimeElapsedLogging.create_log("hyphe_core_retriever",filename="hyphe_core_retriever.log")
    hyphe_core=jsonrpclib.Server(hyphe_core_url)
    while True :
        we=web_entity_pile.get()
        log.log(logging.INFO,"retrieving pages of web entity %s"%(we["name"]))
        web_pages = hyphe_core.store.get_webentity_pages(we["id"])
        log.log(logging.INFO,"retrieved %s pages of web entity %s"%(len(web_pages["result"]),we["name"]))
        we2 = dict(we)
        we2["web_pages"]=web_pages["result"]
        web_page_pile.put(we2)
        web_entity_pile.task_done()
 

def pile_logger(web_entity_pile,web_page_pile,html_code_pile):
    logging.basicConfig(level=logging.DEBUG,
                       filename="piles.log")
    while True :
        logging.log(logging.INFO,"%s items in hyphe_pile, %s items in mongo_pile, %s in solr_pile"%(web_entity_pile.qsize(),web_page_pile.qsize(),html_code_pile.qsize()))
        time.sleep(1)

if __name__=='__main__':
    try:
        with open('config.json') as confile:
            conf = json.loads(confile.read())
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not read configuration\n')
        sys.exit(1)
    try:
        db = pymongo.MongoClient(conf['mongo']['host'], conf['mongo']['port'])
        coll = db[conf["mongo"]["db"]][conf['mongo']['web_pages_collection']]
        coll.ensure_index([('url', pymongo.ASCENDING)], background=True)
        # prepare conte_type filter
        accepted_content_types=[]
        with open(conf['mongo']['contenttype_whitelist_filename']) as content_type_whitelist :
            accepted_content_types=content_type_whitelist.read().split("\n")
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not initiate connection to MongoDB\n')
        sys.exit(1)
    # solr
    try:
        solr = sunburnt.SolrInterface("http://%s:%s/%s" % (conf["solr"]['host'], conf["solr"]['port'], conf["solr"]['path'].lstrip('/')))
        solr.delete_all()
        solr.commit()
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

    try:
        web_entity_queue = JoinableQueue()
        web_entity_urls_queue = JoinableQueue()
        web_entity_htmlcode_queue = JoinableQueue()    
        
        
        hyphe_core_procs=[]
        for _ in range(conf["hyphe2solr"]["nb_hyphe_retriever"]):
            hyphe_core_proc = Process(target=hyphe_core_retriever, args=(web_entity_queue,web_entity_urls_queue, 'http://%s:%s'%(conf["hyphe-core"]["host"],conf["hyphe-core"]["port"])))
            hyphe_core_proc.daemon = True
            hyphe_core_proc.start()
            hyphe_core_procs.append(hyphe_core_proc)

        mongo_procs=[]
        for _ in range(conf["hyphe2solr"]["nb_mongo_retriever"]):
            mongo_proc = Process(target=mongo_retriever, args=(web_entity_urls_queue, web_entity_htmlcode_queue, coll, accepted_content_types))
            mongo_proc.daemon = True
            mongo_proc.start()
            mongo_procs.append(mongo_proc)

        solr_procs=[]
        for _ in range(conf["hyphe2solr"]["nb_indexer"]):
            solr_proc = Process(target=indexer, args=(web_entity_htmlcode_queue, solr))
            solr_proc.daemon = True
            solr_proc.start()
            solr_procs.append(solr_proc)

        
        pile_logger_proc = Process(target=pile_logger,args=(web_entity_queue,web_entity_urls_queue, web_entity_htmlcode_queue,))
        pile_logger_proc.daemon = True
        pile_logger_proc.start()
        
        mainlog=TimeElapsedLogging.create_log("main","main.log")

        web_entity_status=conf["hyphe2solr"]["web_entity_status_filter"]
        nb_web_entities=0
        for status in web_entity_status :
            mainlog.info("retrieving %s web entities"%(status))
            wes=hyphe_core.store.get_webentities_by_status(status)["result"]
            mainlog.info("retrieved %s web entities"%(len(wes)))
            for we in wes:
                web_entity_queue.put(we)
                nb_web_entities+=1
        mainlog.info("retrieved %s web entities"%(nb_web_entities))

        # wait the first provider to finish
        mainlog.log(logging.INFO,"waiting end of web entity pile")
        web_entity_queue.join()

        
        # wait the pile to be processed
        mainlog.log(logging.INFO,"waiting end of mongo pile")
        web_entity_urls_queue.join()

        # wait the second pile to be processed
        mainlog.log(logging.INFO,"web entity pile finished, waiting end of web page pile")
        web_entity_htmlcode_queue.join()
        
        mainlog.log(logging.INFO,"web page pile finished, stopping pile logger, mongo retreiver and solr_proc proc")
        pile_logger_proc.terminate()
        for hyphe_core in hyphe_core_procs:
            hyphe_proc.terminate()
        for mongo_proc in mongo_procs:
            mongo_proc.terminate()
        for solr_proc in solr_procs :
            solr_proc.terminate()
        solr.commit()
        mainlog.log(logging.INFO,"changes to solr index commited")

        solr.optimize()
        mainlog.log(logging.INFO,"Solr index optimized")
    except Exception as e :
        logging.exception("%s %s"%(type(e),e))
    exit(0)
