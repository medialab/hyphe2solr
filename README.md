# principles

This scripts needs :
- one Hyphe server and more precisely :
	- one Hyphe core instance
	- one Hyphe web-pages Mongo instance
- one SOLR node ready to index web-pages

What it does is :
- main process does : 
	- get web entities info from Hyphe core
	- get web pages list from Hyphe core
	- put ULR list by web entity in WEB_ENTITY_PILE
- mongo process does : 
	- depile a web_entity url list from WEB_ENTITY_PILE
	- retrieve the mongo document for all URLs (filtering on mimetype)
	- put ready-to-index documents in WEB_PAGE_PILE
- solr process does :
	- depile documents to index
	- batch index them

# configuration

## hyphe SOLR schema

use the solr node example provided in solr_hyphe_core directory.
the script deploy_solr_core.sh might helps you.
Change the solr core path in the script before using it.
BEWARE : It will erase any hyphe core already present in solr core path.
This script implies you ursing tomcat7.

	You should review the script before using it.

## connection to data sources 

Copy config.json.default into config.json and edit the parameters :
- host/port of Hyphe core
- host/port/db/collection of mongo hyphe db
- host/port/path of solr node

## Mime-type filter

Hyphe2solr proposes you to filter out web pages which doesn't have a mimetype compatible with solr indexing (our schema don't use TIKKA).
The script generate_content_filter.py outputs from the mongodb a CSV liting the cotent-type ordered by number of pages found in the mongo.
From this csv you have to write the content_type_whitelist.txt file.
This file must contain one mimetype (to be indexed) by line.
An example is provided : content_type_whitelist.txt.default

# dependencies

## HYPHE

see 

## SOLR

see

## python requirements

sunburnt
lxml
httplib2
pymongo
jsonrpclib

# INSTALL

You need a hyphe and a solr server running.

Than simply execute (ideally in a virtualenv): 

	pip install -r requirements.txt

# usage

Once you prepared the configuration, simply use : 

	python run.py

# logs

Hyphe2solr feed 5 logs : 

- main.log : the main process monitoring the chain of action
- piles.log : monitoring the number of items left in piles to be treated
- hyphe_core_retriever.log : monitor communications with Hyphe core
- mongo_retriever.log : monitor communications with mongo
- solr_index.log : monitor communication with solr
