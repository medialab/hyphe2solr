#!/usr/bin/env python
# -*- coding: utf-8 -*-

# system and utils
import sys, re, json, os, time
import logging
import pymongo
from argparse import ArgumentParser
from sunburnt import SolrInterface
from hashlib import md5

# logging
import TimeElapsedLogging

re_find_domain = re.compile(r'^(https?://[^/]*/).*$')

def index_pages(pages, total, solr, corpus):
    processlog = TimeElapsedLogging.create_log("corpus",filename="logs/by_corpus/%s.log" % corpus)
    errors_solr_document_filename = "logs/errors_solr_document/%s.json" % corpus
    nb_pages_indexed = 0
    nb_errors = 0
    sys.stdout.write("\r%s items left to process in page pile for %s " % (total, corpus))
    sys.stdout.flush()
    t = time.time()
    for page in pages:
       # for k in page:
       #     page[k] = page[k].decode('utf-8')
        # logging in proc log
        processlog.info("%s: starting processing" % corpus)
        domain = page['site'] #re_find_domain.sub(r'\1', page["permalink"])
        solr_document={
            "id": page["permalink"],          # use url as id
            "site": domain,       # use domain for we
            "corpus": corpus,         # use source file as corpus (i.e. ONGs/Institutions here)
            "url": page["permalink"],         # actual url
            "categories": " ".join(page["category"].split('/')),    # keywords queried leading to this page
            "text": page['plain_content'],        # already extracted text
            "date": page['date'],        # already extracted text
            "year": page['date'].isoformat()[:4],        # already extracted text
            "title": page['title']        # already extracted text
        }
        try:
             solr.add(solr_document)
             nb_pages_indexed += 1
        except Exception as e:
            nb_errors += 1
            page['exception'] = "%s: %s" % (type(e), e)
            processlog.debug("Exception with webpage %s : %s" % (page["permalink"], page['exception']))
            print >> sys.stderr, "ERROR", page
            #with open(errors_solr_document_filename,"a") as errors_solr_document_json_file:
            #    json.dump(page, errors_solr_document_json_file, indent=4)
        if time.time() - t > 1:
            sys.stdout.write("\r%s items left to process in page pile for %s (%s errors so far)" % (total - nb_pages_indexed - nb_errors, corpus, nb_errors))
            sys.stdout.flush()
    processlog.info("%s: indexed %s web pages" % (corpus,nb_pages_indexed))
    #solr.commit()
    #relying on autocommit

if __name__=='__main__':

    # usage :
    # --delete_index
    parser = ArgumentParser()
    parser.add_argument("-d","--delete_index", action='store_true', help="delete solr index before (re)indexing.\n\rWARNING all previous indexing work will be lost.")
    args = parser.parse_args()

    mainlog=TimeElapsedLogging.create_log("main")
    try:
        with open('config.json') as confile:
            conf = json.loads(confile.read())
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not read configuration\n')
        sys.exit(1)

    try:
        if not os.path.exists("logs"):
            os.makedirs("logs")
            os.makedirs("logs/by_corpus")
            os.makedirs("logs/errors_solr_document")
        if args.delete_index:
            #delete the processed list
            for f in os.listdir("logs/by_corpus"):
            	os.remove(os.path.join("logs/by_corpus",f))
            for f in os.listdir("logs/errors_solr_document"):
                os.remove(os.path.join("logs/errors_solr_document",f))
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not create logs directory\n')
        sys.exit(1)

    #mongodb
    try:
        db = pymongo.MongoClient(conf['mongo']['host'], conf['mongo']['port'])
        coll = db[conf["mongo"]["db"]][conf['mongo']['collection']]
        pages_it = coll.find()
        total = coll.count()
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not initiate connection to MongoDB\n')
        sys.exit(1)
    # solr
    try:
        solr = SolrInterface("http://%s:%s/%s" % (conf["solr"]['host'], conf["solr"]['port'], conf["solr"]['path'].lstrip('/')))
        if args.delete_index:
            solr.delete_all()
            solr.commit()
    except Exception as e:
        print type(e), e
        sys.stderr.write('ERROR: Could not initiate connection to SOLR node\n')
        sys.exit(1)

    try:
        mainlog.info("resuming indexation on %s corpus" % conf['solr']['path'])
        index_pages(pages_it, total, solr, conf["mongo"]['collection'])
        solr.commit()
        mainlog.log(logging.INFO,"last solr comit to be sure")

        solr.optimize()
        mainlog.log(logging.INFO,"Solr index optimized")
    except Exception as e :
        logging.exception("%s %s"%(type(e),e))
