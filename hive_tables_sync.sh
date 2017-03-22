#!/usr/bin/bash

currentDir=/root/hive_metadata_copy
DATE_PART=`date +'%Y%m%d%H%M%S'`
ddlfile=$currentDir/file_$DATE_PART.ddl
hqlDir=$currentDir/hqlDir
mysql -s -h `hostname -f` -u hive_readonly -p -e "select concat( 'show create table ' , T.NAME , '.', T.TBL_NAME,';') from (select DBS.NAME, TBLS.TBL_NAME from TBLS left join DBS on TBLS.DB_ID = DBS.DB_ID where DBS.NAME='default') T" hive > $ddlfile


while read i;
do
        tableName=`echo $i | awk '{ print $4}' | awk --field-separator=";" '{print $1}'`;
        hqlfile=$hqlDir/$tableName.hql
        hive  --hiveconf hive.metastore.uris=thrift://<<soucrcehive>>:9083 -e "$i" > $hqlfile;
        sed -i 's/hdfs\:\/\/<<NameNode service name for source hdfs>>//g' $hqlfile
        hive --hiveconf hive.metastore.uris=thrift://<<destination_hive>>:9083 -f $hqlfile
        hive --hiveconf hive.metastore.uris=thrift://<<destination_hive>>:9083 "ANALYZE TABLE $tableName COMPUTE STATISTICS;"
done < $ddlfile
