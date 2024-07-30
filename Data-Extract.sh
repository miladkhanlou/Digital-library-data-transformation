#! /bin/bash

cd /var/www/drupal7

drush -u 1 islandora_datastream_crud_fetch_pids --namespace=COLLECTION_NAME --pid_file=/tmp/COLLECTION_NAME.txt
drush -u 1 idcrudfd --pid_file=/tmp/COLLECTION_NAME.txt --datastreams_directory=/tmp/COLLECTION_NAME --dsid=MODS
drush -u 1 idcrudfd --pid_file=/tmp/COLLECTION_NAME.txt --datastreams_directory=/tmp/COLLECTION_NAME --dsid=RELS-EXT
drush -u 1 idcrudfd --pid_file=/tmp/COLLECTION_NAME.txt --datastreams_directory=/tmp/COLLECTION_NAME --dsid=OBJ
