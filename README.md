# principles

This scripts needs :
- one Hyphe server and more precisely :
	- one Hyphe core instance
	- one Hyphe web-pages Mongo instance
- one SOLR node ready to index web-pages

What it does is :
- get web entities info from Hyphe core filtering by status (see configuration)
- put the web entities not already processed (see logs) into WEB_ENTITY_PILE
- start nb_process (see configuration) processes to work on the web entities retrieved :
	- get a web entity from WEB_ENTITY_PILE 	
	- get web pages list from Hyphe core
	- retrieve the mongo document for all URLs (filtering on mimetype see configuration)
	- prepare documents to be indexed by creating a text verison of the HTML code (see html2text.py) 
	- index documents


# dependencies

## HYPHE

This script relies on an existing Hyphe server running.
see https://github.com/medialab/Hypertext-Corpus-Initiative

## SOLR

This script relies on an existing solr server running.
see https://lucene.apache.org/solr/

## python requirements

sunburnt
lxml
httplib2
pymongo
jsonrpclib
argparse #for python<2.7

# INSTALL

You need a hyphe and a solr server running.

Than simply execute (ideally in a virtualenv): 

	pip install -r requirements.txt

# CONFIGURE

## hyphe SOLR schema

use the solr node example provided in solr_hyphe_core directory.
the script deploy_solr_core.sh might helps you.
Change the solr core path and tomcat user/service (depends on your install) in the script before using it.
BEWARE : It will erase any hyphe core already present in solr core path.

	You should review the script before using it.

## connection to data sources 

Copy config.json.default into config.json and edit the parameters :
- hyphe2core :
	- "nb_process": number of concurrent process to start
	- "web_entity_status_filter: a web entity filter to index based on hyphe status
- host/port of Hyphe core
- host/port/db/collection of mongo hyphe db
- host/port/path of solr node

## Mime-type filter

Hyphe2solr proposes you to filter out web pages which doesn't have a mimetype compatible with solr indexing (our schema don't use TIKKA).
The script generate_content_filter.py outputs from the mongodb (version >2.1 only) a CSV listing the cotent-type ordered by number of pages found in the mongo.
From this csv you have to write the content_type_whitelist.txt file.
This file must contain one mimetype (to be indexed) by line.
An example is provided :
	content_type_whitelist.txt.default

# usage

Once you prepared the configuration, simply use : 

	$ python index_hyphe_web_pages.py

Only one option which delete the existing index before (re)indexing

	$ python index_hyphe_web_pages.py -h
	usage: index_hyphe_web_pages.py [-h] [-d]

	optional arguments:
	  -h, --help          show this help message and exit
	  -d, --delete_index  delete solr index before (re)indexing. WARNING all
	                      previous indexing work will be lost.

If calling index_hyphe_web_pages.py multiple times without -d|--delete_index option, the indexation process will omit the web entities listed by id in logs/we_id_done.log
The defautl behaviour is thus to resume any previous unfinished indexations.

# logs

Hyphe2solr logs into 3 log directories : 

- ./logs/by_pid/ : one log file by process
- ./logs/by_web_entity/ : one log file by web entity indexed
- ./logs/errors_solr_document/ : logs documents the script couldn't index in Solr

Hyphe2solr outputs the ids of indexed web entities in :
- ./logs/we_id_done.log : this file is used to resume indexing operations from where it stopped

When using -d or --delete_index option, the script clears all the logs.
