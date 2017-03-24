#!/usr/bin/bash

if [ "$#" -ne 1 ]
then
  echo "Usage: $0 <Hive dbname for which all tables needs to be created>"
  exit 1
fi

currentDir=/tmp/hive_metadata_copy
DATE_PART=`date +'%Y%m%d%H%M%S'`
ddlfile=$currentDir/file_$DATE_PART.ddl
hqlDir=$currentDir/hqlDir
HiveDbName=$1
mkdir $hqlDir
mysql -s -h <<sourceHiveMySQLHost>> -u hive_readonly -p -e "select concat( 'show create table ' , T.NAME , '.', T.TBL_NAME,';') from (select DBS.NAME, TBLS.TBL_NAME from TBLS left join DBS on TBLS.DB_ID = DBS.DB_ID where DBS.NAME='$HiveDbName') T" hive > $ddlfile


while read i;
do
	tableName=`echo $i | awk '{ print $4}' | awk --field-separator=";" '{print $1}'`;
	hqlfile=$hqlDir/$tableName.hql
	hive  --hiveconf hive.metastore.uris=thrift://<<SourceHiveMetaStore>>:9083 -e "$i" > $hqlfile;
  #Uncomment below if NameNode is in HA
	#sed -i 's/hdfs\:\/\/<<Source_NameNodeServiceName>>//g' $hqlfile
 	sed -i '/TBLPROPERTIES/Q' $hqlfile
	hive --hiveconf hive.metastore.uris=thrift://<<DestHiveMetaStore>>:9083 -f $hqlfile
#	hive --hiveconf hive.metastore.uris=thrift://<<DestHiveMetaStore>>:9083 "ANALYZE TABLE $tableName COMPUTE STATISTICS;"
done < $ddlfile
