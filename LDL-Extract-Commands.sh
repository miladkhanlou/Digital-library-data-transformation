#! /bin/bash

cd /var/www/drupal7

drush -u 1 islandora_datastream_crud_fetch_pids --namespace=COLLECTION_NAME --pid_file=/tmp/COLLECTION_NAME.txt
drush -u 1 idcrudfd --pid_file=/tmp/COLLECTION_NAME.txt --datastreams_directory=/tmp/COLLECTION_NAME --dsid=MODS
drush -u 1 idcrudfd --pid_file=/tmp/COLLECTION_NAME.txt --datastreams_directory=/tmp/COLLECTION_NAME --dsid=RELS-EXT
drush -u 1 idcrudfd --pid_file=/tmp/COLLECTION_NAME.txt --datastreams_directory=/tmp/COLLECTION_NAME --dsid=OBJ

#
cd /tmp/COLLECTION_NAME
zip COLLECTION_NAME.zip *

ctl-D

scp dgi-ingest:/tmp/COLLECTION_NAME/COLLECTION_NAME.zip ~/Downloads
cp ~/Downloads/COLLECTION_NAME.zip /meda/wwc/0A2C-888E/COLLECTION_NAME/

cd /media/wwc/0A2C-888E/amistad-pgoudvis
mkdir Data
cd Data
unzip ../COLLECTION_NAME.zip 

for f in *.jp2; do opj_decompress -i "$f" -OutFor PNG -o "${f%.*}.png"; done;