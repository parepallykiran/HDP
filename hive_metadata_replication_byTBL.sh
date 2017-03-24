#!/usr/bin/bash
# Script to create Hive table from one cluster to other cluster with specific list of Hive tables


if [ "$#" -ne 1 ]
then
  echo "Usage: $0 <Path to file with list of tables, Format dbname.tablename>"
  exit 1
fi

currentDir=`pwd`
DATE_PART=`date +'%Y%m%d%H%M%S'`
tblList=$1
hqlDir=$currentDir/hqlDir
[[ -d $hqlDir ]] || mkdir $hqlDir

while read i;
do
	tableName=$i
	hqlfile=$hqlDir/$tableName.hql
	hive  --hiveconf hive.metastore.uris=thrift://<<source_hivemetastore>>:9083 -e "show create table $i" > $hqlfile;
	#Uncomment below for HA cluster and update source Namenode service name
  #sed -i "s/hdfs\:\/\/<<source HDFS Service Name>>//g" $hqlfile
	sed -i '/TBLPROPERTIES/Q' $hqlfile
	hive --hiveconf hive.metastore.uris=thrift://<<destination_hivemetastore>>:9083 -f $hqlfile
done < $tblList
