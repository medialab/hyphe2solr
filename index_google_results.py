#!/usr/bin/env python
# -*- coding: utf-8 -*-

# system and utils
import sys, re, json, os, time
import logging
from argparse import ArgumentParser
import csv
from sunburnt import SolrInterface
from hashlib import md5


# logging
import TimeElapsedLogging

csv.field_size_limit(sys.maxsize)

re_find_domain = re.compile(r'^(https?://[^/]*/).*$')

def index_pages(path, solr):
    processlog = TimeElapsedLogging.create_log("corpus",filename="logs/by_corpus/%s.log" % corpus)
    errors_solr_document_filename = "logs/errors_solr_document/%s.json" % corpus
    with open(path, "r") as pagesfile:
        pages = csv.DictReader(pagesfile)
        total_pages = 0
        for page in pages:
            total_pages += 1
        pagesfile.seek(0)
        nb_pages_indexed = 0
        nb_errors = 0
        sys.stdout.write("\r%s items left to process in page pile for %s " % (total_pages, corpus))
        sys.stdout.flush()
        t = time.time()
        for page in pages:
            for k in page:
                page[k] = page[k].decode('utf-8')
            # logging in proc log
            processlog.info("%s: starting processing" % filename)
            domain = re_find_domain.sub('', page["url"])
            html_id = md5("%s\n" % page['url']).hexdigest()
            solr_document={
                "id": page["url"],          # use url as id
                "web_entity": domain,       # use domain for we
                "web_entity_id": domain,    # use domain as we id
                "web_entity_status": "IN",  # everything here is IN
                "corpus": filename,         # use source file as corpus (i.e. ONGs/Institutions here)
                "encoding": "UTF8",         # always UTF8 with text already extracted
                "original_encoding": page["format"],    # format of source
                "url": page["url"],         # actual url
                "lru": page["keywords"],    # keywords queried leading to this page
                "depth": 1,                 # no use here, default
                "html": "%s.%s" % (html_id, page['format']),    # id of file containing full html/pdf source
                "text": page['text']        # already extracted text
            }
            try:
                 solr.add(solr_document)
                 nb_pages_indexed += 1
            except Exception as e:
                nb_errors += 1
                page['exception'] = "%s: %s" % (type(e), e)
                processlog.debug("Exception with webpage %s : %s" % (page["url"], page['exception']))
                with open(errors_solr_document_filename,"a") as errors_solr_document_json_file:
                    json.dump(page, errors_solr_document_json_file, indent=4)
            if time.time() - t > 1:
                sys.stdout.write("\r%s items left to process in page pile for %s (%s errors so far)" % (total_pages - nb_pages_indexed - nb_errors, corpus, nb_errors))
                sys.stdout.flush()
        processlog.info("%s: indexed %s web pages" % (filename,nb_pages_indexed))
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
        mainlog.info("resuming indexation on %s corpora with text extraction type %s" % (len(conf['google_results']['corpora']), conf['google_results']['type_text']))

        for corpus in conf['google_results']['corpora']:
            if conf['google_results']['type_text'] == 'raw':
                  filename = "6-corpus_results_text_raw.csv"
            else: filename = "7-corpus_results_text_canola.csv"
            filepath = os.path.join("%s%s" % (conf['google_results']['csvpath'], corpus), filename)
            index_pages(filepath, solr)
        solr.commit()
        mainlog.log(logging.INFO,"last solr comit to be sure")

        solr.optimize()
        mainlog.log(logging.INFO,"Solr index optimized")
    except Exception as e :
        logging.exception("%s %s"%(type(e),e))
