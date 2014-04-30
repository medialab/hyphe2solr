#!/bin/bash

solr_core=$(grep '"path":' config.json | head -n 1 | sed 's#^.*/\(.*\)"\s*$#\1#')
service tomcat6 stop
solr_data_path="/store/solr-data"
if [ -z "$solr_core" ]; then
  echo "WARNING config seems badly set"
  exit 1
fi
rm -rf $solr_data_path/$solr_core
mkdir $solr_data_path/$solr_core
cp -R solr_hyphe_core/* $solr_data_path/$solr_core/
echo "name=$solr_core" > $solr_data_path/$solr_core/core.properties

chown -R tomcat:tomcat $solr_data_path/$solr_core
service tomcat6 start
