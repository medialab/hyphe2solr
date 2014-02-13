# principles

This scripts needs :
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
	- retrieve the mongo document for all URLs (filtering one mimetype)
	- put ready-to-index documents in WEB_PAGE_PILE
- solr process does :
	- depile documents to index
	- batch index them

# configuration

## create SOLR node

use the solr node example.

## data sources 
The scripts needs to know : 
- host port of Hyphe core
- host port of mongo hyphe db
- host port of solr node

# dependencies



# INSTALL

to be completed

	pip install -r requirements.txt

# Process

## SOLR index