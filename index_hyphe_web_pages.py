#!/usr/bin/env python
# -*- coding: utf-8 -*-

# system and utils
import sys, time, re, json, os, shutil
from multiprocessing import Process, JoinableQueue
import logging
import html2text
import argparse
import signal

# data sources
import pymongo
import sunburnt
import jsonrpclib

# logging
import TimeElapsedLogging


def index_webentity(web_entity_pile,web_entity_done_pile,conf,mainlog):
    processlog=TimeElapsedLogging.create_log(str(os.getpid()),filename="logs/by_pid/%s.log"%os.getpid())
    processlog.info("starting infinite loop")
    corpus = conf['hyphe-core']['corpus_id']
    solr = sunburnt.SolrInterface("http://%s:%s/solr/%s" % (conf["solr"]['host'], conf["solr"]['port'], get_solr_instance_name(conf["solr"]['path'])))
    hyphe_core=jsonrpclib.Server('http://%s:%s'%(conf["hyphe-core"]["host"], conf["hyphe-core"]["port"]), version=1)
    db = pymongo.MongoClient(conf['mongo']['host'], conf['mongo']['port'])
    collname = "%s.pages" % conf['hyphe-core']['corpus_id']
    coll = db[conf["mongo"]["db"]][collname]
    while True :
        we=web_entity_pile.get()

        # logging in proc log
        processlog.info("%s: starting processing"%we["name"])

        #setting LOG
        web_entity_name_safe=re.sub(r"[\W]","",we['name'])
        web_entity_log_id="%s_%s"%(web_entity_name_safe,we["id"])
        logfilename="logs/by_web_entity/%s.log"%web_entity_log_id
        errors_solr_document_filename="logs/errors_solr_document/%s.json"%web_entity_log_id
        welog=TimeElapsedLogging.create_log(we["id"],filename=logfilename)

        #getting web pages URLS
        welog.log(logging.INFO,"retrieving pages of web entity %s"%(we["name"]))
        #mainlog.info("DEBUG %s"%(we["id"]))
        web_pages = hyphe_core.store.get_webentity_pages(we["id"], True, corpus)
        if (web_pages['code'] == 'fail') :
            mainlog.info(we_pages['message'])
        welog.log(logging.INFO,"retrieved %s pages of web entity %s"%(len(web_pages["result"]),we["name"]))
        we["web_pages"]=web_pages["result"]

        processlog.info("%s: got %s webpages"%(we["name"],len(we["web_pages"])))

        #getting mongo html web page
        urls=[page["url"] for page in we["web_pages"]] #if page["http_status"]!=0]
        nb_urls=len(urls)
        last_id=""
        pages_mongo=[]
        nb_pages_mongo=0
        nb_pages_indexed=0
        i=0
        url_slice_len=1000
        welog.info("retrieving + indexing HTML pages from mongo to solr of web entity %s"%(we["name"]))

        while i<len(urls) :
            urls_slice=urls[i:i+url_slice_len]
            pages_mongo_slice=list(coll.find({
                    "url": {"$in": urls_slice},
                    "status": 200,
                    "content_type": {"$in": accepted_content_types},
                    "body" : {"$exists":True}
                },
                projection=["_id","encoding","url","lru","depth","body"]))
            #mainlog.info(str(len(pages_mongo_slice)))
            #local counters
            nb_slice_mongo=len(pages_mongo_slice)
            nb_slice_indexed=0

            welog.info("%s %s: got %s pages in slice %s %s"%(we["name"],we["id"],nb_slice_mongo,i,len(urls_slice)))

            error_solr_doc=[]
            for page_mongo in pages_mongo_slice:
                body = page_mongo["body"].decode('zip')
                try:
                    body = body.decode(page_mongo.get("encoding",""))
                    encoding = page_mongo.get("encoding","")
                except Exception :
                    body = body.decode("UTF8","replace")
                    encoding = "UTF8-replace"
                solr_document={
                    "id":page_mongo["_id"],
                    "web_entity":we["name"],
                    "web_entity_id":we["id"],
                    "web_entity_status":we["status"],
                    "corpus":conf['hyphe-core']['corpus_id'],
                    "encoding":encoding,
                    "original_encoding":page_mongo.get("encoding",""),
                    "url":page_mongo["url"],
                    "lru":page_mongo["lru"],
                    "depth":page_mongo["depth"],
                    #"html":body,
                    "text":html2text.textify(body, encoding)
                }

                try:
                     solr.add(solr_document)
                     nb_slice_indexed+=1
                except Exception as e:
                    # mainlog.info("ERROR %s: %s %s" %(type(e),e, solr_document))
                    #welog.debug("Exception with document :%s %s %s"%(solr_document["id"],solr_document["url"],solr_document["encoding"]))
                    error_solr_doc.append({"error": "%s: %s" % (type(e), e), "url":solr_document["url"],"encoding":solr_document["encoding"],"original_encoding":solr_document["original_encoding"]})
                    # import traceback
                    # traceback.print_exc()
            if len(error_solr_doc) >0 :
                with open(errors_solr_document_filename,"a") as errors_solr_document_json_file :
                    json.dump(error_solr_doc,errors_solr_document_json_file,indent=4)
            del(error_solr_doc)
			#log
            welog.info("%s %s: indexed %s pages"%(we["name"],we["id"],nb_slice_indexed))
            #processlog.info("indexed %s html pages for %s"%(nb_slice_indexed,(we["name"])))
            # global counters
            nb_pages_mongo+=nb_slice_mongo
            nb_pages_indexed+=nb_slice_indexed
            i=i+url_slice_len


        del we["web_pages"]
        del web_pages
        del urls

        welog.log(logging.INFO,"'%s' indexed (%s web pages on %s)"%(we["name"],nb_pages_indexed,nb_pages_mongo))
        try:
            solr.commit()
        except Exception as e:
            mainlog.info("ERROR %s: %s" %(type(e), e))
            mainlog.info("Retrying...")
            try:
                solr.commit()
            except Exception as e:
                mainlog.info("STILL BROKEN, giving up on %s %s" % (we['id'], we['name']))
		#relying on autocommit
        #welog.info("inserts to solr comited")
        processlog.info("%s: indexed %s on %s Html pages"%(we["name"],nb_pages_indexed, nb_pages_mongo))
        #adding we if to done list
        web_entity_done_pile.put(we["id"])
        del we
        web_entity_pile.task_done()


def pile_logger(web_entity_pile):
    while True :
        time.sleep(1)
        sys.stdout.write("\r%s items in web entity pile" %web_entity_pile.qsize())
        sys.stdout.flush()

def writing_we_done(web_entity_done_pile):
    with open("logs/we_id_done.log","a") as we_id_done_file:
        while True:
            we_id=web_entity_done_pile.get()
            we_id_done_file.write("%s\n"%we_id)
            we_id_done_file.flush()
            web_entity_done_pile.task_done()

re_solrname = re.compile(r"^.*/([^/]+)$")
get_solr_instance_name = lambda solrpath: re_solrname.sub(r"\1", solrpath)


if __name__=='__main__':

    # usage :
    # --delete_index
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--delete_index", action='store_true', help="delete solr index before (re)indexing.\n\rWARNING all previous indexing work will be lost.")
    args = parser.parse_args()


    mainlog=TimeElapsedLogging.create_log("main")
    #Load conf
    try:
        with open('config.json') as confile:
            conf = json.loads(confile.read())
    except Exception as e:
        sys.stderr.write("%s: %s\n" % (type(e), e))
        sys.stderr.write('ERROR: Could not read configuration\n')
        sys.exit(1)

    try:
        if not os.path.exists("logs"):
            os.makedirs("logs")
            os.makedirs("logs/by_pid")
            os.makedirs("logs/by_web_entity")
            os.makedirs("logs/errors_solr_document")
        if args.delete_index:
            #delete the processed web entity list
            if os.path.isfile("logs/we_id_done.log"):
				os.remove("logs/we_id_done.log")
            for f in os.listdir("logs/by_pid"):
            	os.remove(os.path.join("logs/by_pid",f))
            for f in os.listdir("logs/by_web_entity"):
				os.remove(os.path.join("logs/by_web_entity",f))
            for f in os.listdir("logs/errors_solr_document"):
                os.remove(os.path.join("logs/errors_solr_document",f))

    except Exception as e:
        sys.stderr.write("%s: %s\n" % (type(e), e))
        sys.stderr.write('ERROR: Could not create log directory\n')
        sys.exit(1)

    #mongodb
    try:
        db = pymongo.MongoClient(conf['mongo']['host'], conf['mongo']['port'])
        collname = "%s.pages" % conf['hyphe-core']['corpus_id']
        coll = db[conf["mongo"]["db"]][collname]
        mongo_index=[]
        mainlog.info("creating mongo indexes")
        mongo_index.append(coll.create_index([('url', pymongo.ASCENDING)], background=True))
        mainlog.info('index on url done')
        mongo_index.append(coll.create_index([('content_type', pymongo.ASCENDING)], background=True))
        mainlog.info("index on content_type done")
        # prepare conte_type filter
        accepted_content_types=[]
        with open(conf['mongo']['contenttype_whitelist_filename']) as content_type_whitelist :
            accepted_content_types=content_type_whitelist.read().split("\n")
    except Exception as e:
        sys.stderr.write("%s: %s\n" % (type(e), e))
        sys.stderr.write('ERROR: Could not initiate connection to MongoDB\n')
        sys.exit(1)
    # solr
    try:
        solr = sunburnt.SolrInterface("http://%s:%s/solr/%s" % (conf["solr"]['host'], conf["solr"]['port'], get_solr_instance_name(conf["solr"]['path'])))
        if args.delete_index:
            solr.delete_all()
            solr.commit()
    except Exception as e:
        sys.stderr.write("%s: %s\n" % (type(e), e))
        sys.stderr.write('ERROR: Could not initiate connection to SOLR node\n')
        sys.exit(1)
    # hyphe core
    try:
        hyphe_core=jsonrpclib.Server('http://%s:%s'%(conf["hyphe-core"]["host"], conf["hyphe-core"]["port"]), version=1)
    except Exception as e:
        sys.stderr.write("%s: %s\n" % (type(e), e))
        sys.stderr.write('ERROR: Could not initiate connection to hyphe core\n')
        sys.exit(1)

    res = hyphe_core.ping(conf['hyphe-core']['corpus_id'], 10)
    if "message" in res:
        sys.stderr.write("ERROR: please start or create corpus %s before indexing it: %s\n" % (conf['hyphe-core']['corpus_id'], res['message']))
        sys.exit(1)

    try:
        web_entity_queue = JoinableQueue()
        web_entity_done = JoinableQueue()


        hyphe_core_procs=[]
        for _ in range(conf["hyphe2solr"]["nb_process"]):
            hyphe_core_proc = Process(target=index_webentity, args=(web_entity_queue,web_entity_done,conf,mainlog))
            hyphe_core_proc.daemon = True
            hyphe_core_proc.start()
            hyphe_core_procs.append(hyphe_core_proc)


        pile_logger_proc = Process(target=pile_logger,args=(web_entity_queue,))
        pile_logger_proc.daemon = True
        pile_logger_proc.start()


        web_entity_status=conf["hyphe2solr"]["web_entity_status_filter"]
        nb_web_entities=0
        for status in web_entity_status :
            mainlog.info("retrieving %s web entities"%(status))
            wes = []
            res = hyphe_core.store.get_webentities_by_status(status, None, 500, 0, conf['hyphe-core']['corpus_id'])["result"]
            wes += res['webentities']
            while res["next_page"]:
                res = hyphe_core.store.get_webentities_page(res["token"], res["next_page"], conf['hyphe-core']['corpus_id'])["result"]
                wes += res['webentities']

            mainlog.info("retrieved %s web entities"%(len(wes)))
            try:
                with open("logs/we_id_done.log","r") as we_id_done_file:
                    we_id_done=we_id_done_file.read().split("\n")
            except:
                we_id_done=[]
            for we in wes :
                if we["id"] not in we_id_done:
                    we["status"]=status
                    web_entity_queue.put(we)
                    nb_web_entities+=1
        mainlog.info("resuming indexation on %s web entities"%(nb_web_entities))

        writing_we_done_proc = Process(target=writing_we_done,args=(web_entity_done,))
        writing_we_done_proc.daemon = True
        writing_we_done_proc.start()

        # wait the first provider to finish
        mainlog.log(logging.INFO,"waiting end of web entity pile")
        web_entity_queue.join()

        mainlog.log(logging.INFO,"waiting end of web entity done writing pile")
        web_entity_done.join()


        mainlog.log(logging.INFO,"web page pile finished, stopping pile logger, mongo retreiver and solr_proc proc")
        pile_logger_proc.terminate()
        writing_we_done_proc.terminate()
        for hyphe_proc in hyphe_core_procs:
            hyphe_proc.terminate()
        for index in mongo_index:
            coll.drop_index(index)

        solr.commit()
        mainlog.log(logging.INFO,"last solr comit to be sure")

        solr.optimize()
        mainlog.log(logging.INFO,"Solr index optimized")
    except Exception as e :
        logging.exception("%s %s"%(type(e),e))
    sys.exit(0)
