#!/bin/bash

service tomcat7 stop
solr_data_path="/usr/share/solr/example/solr/"
rm -rf $solr_data_path/hyphe
cp solr_hyphe_core $solr_data_path/hyphe
chown -R tomcat7:tomcat7 $solr_data_path/hyphe/*
chown tomcat7:tomcat7 $solr_data_path/hyphe
service tomcat7 start