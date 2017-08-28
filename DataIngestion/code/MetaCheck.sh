#!/bin/bash
#   Metadata check and trigger of data ingestion
#	Event handler to identify new request from Data-Transport API and trigger DI
#	Author: Siddharth Sharma     
#		                                              
#       $1 = properties file name passed with parameters 

REFID=$1
DATASOURCEID=$2
DATALOCATION=$3

PRIORITYFLAG=$4
TABLENAME=$5
DELIMITER=$6
BODY_PATH_REC=$7
load_id=${8}
CONF_FILE=${9}
POLLTIME=${10}
##poc
DATASET=$DATASOURCEID
RAW_LOCATION=$DATALOCATION
PIG_INPUT=$DESTINATION_BUCKET_PATH_PIG

. $CONF_FILE

TABLENAME=${TABLENAME^^}
DB=${md_db_name^^}

if [ ! -d "$PARALLEL_FILE_DIR" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir $PARALLEL_FILE_DIR
fi
#creating file for maintaining parallelism
touch $PARALLEL_FILE_DIR/Parallel_$REFID.txt
if [ $? -ne 0 ]; then
	echo "touch file couldn't get created... exiting..."
	exit 1
fi
hive -e "insert into table $DB.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'Started',current_timestamp() "

METADATA_ENTRY=0
hive -e "select count(distinct td.tableid) from $DB.dataset_metadata dm inner join $DB.table_definition td where dm.dataset='$DATASET' and td.raw_table_name='$TABLENAME' and td.table_name=dm.table_name and td.table_dataset=dm.dataset" > $SQL_OUT_PATH/dataset_metadata_${dataset}_${TABLENAME}_${POLLTIME}.out

METADATA_ENTRY=`cat $SQL_OUT_PATH/dataset_metadata_${dataset}_${TABLENAME}_${POLLTIME}.out| tr -s " "`

METADATA_COLUMN_ENTRY=0
hive -e "select count(distinct cd.tableid) from $DB.column_definition cd where cd.tableid in (select distinct td.tableid from $DB.dataset_metadata dm inner join $DB.table_definition td on td.table_name=dm.table_name and td.table_dataset=dm.dataset where dm.dataset='$DATASET' and td.raw_table_name='$TABLENAME')" > $SQL_OUT_PATH/dataset_column_metadata_${dataset}_${TABLENAME}_${POLLTIME}.out

METADATA_COLUMN_ENTRY=`cat $SQL_OUT_PATH/dataset_column_metadata_${dataset}_${TABLENAME}_${POLLTIME}.out| tr -s " "`

##validating the metadata
if [ $METADATA_ENTRY -eq 0 ] || [ $METADATA_ENTRY -ne $METADATA_COLUMN_ENTRY ]; then
	echo "technical Metadata is not ready for data source ID: $DATASOURCEID and table name $TABLENAME"
	hive -e "insert into table $DB.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'Failed',current_timestamp() "
	rm $PARALLEL_FILE_DIR/Parallel_$REFID.txt
	rm $BODY_PATH_REC
	sub="Data Ingestion failed for Data Source ID $DATASOURCEID and table name $TABLENAME"
	echo "Data Ingestion failed for Data Source ID $DATASOURCEID and table name $TABLENAME because technical Metadata is not available" >>$BODY_PATH_REC
	echo "Date:" `date +%Y-%m-%d' '%H:%M:%S:%3N` >>$BODY_PATH_REC
	rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
	rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out

	echo "$to $sub $BODY_PATH_REC $DATASET $TABLENAME"
	#sh $SCRIPTS_PATH/mail_alert_script.sh $to "$sub" $BODY_PATH_REC $DATASET $TABLENAME $SQL_OUT_PATH
	hive -e "insert into table $DB.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'Failed - Technical Metadata is not available',current_timestamp() "
	exit 1
fi

rm $BODY_PATH_REC
rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out
sub="Data Ingestion started for Dataset $DATASET and table name $TABLENAME"
echo "Data Ingestion started for Dataset $DATASET and table name $TABLENAME" >>$BODY_PATH_REC
echo "Date:" `date +%Y-%m-%d' '%H:%M:%S:%3N` >>$BODY_PATH_REC
#sh $SCRIPTS_PATH/mail_alert_script.sh $to "$sub" $BODY_PATH_REC $DATASET $TABLENAME $SQL_OUT_PATH
hive -e "insert into table $DB.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'DI Started',current_timestamp() "

sh $SCRIPTS_PATH/load_dataset.sh $DATASET $CONF_FILE $RAW_LOCATION $TABLENAME "$DELIMITER" $POLLTIME $PIG_INPUT $S3_LOCATION

if [ $? -ne 0 ]; then
	echo "Data Ingestion Failed for $REFID and $DATASET"
	hive -e "insert into table $DB.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'Failed',current_timestamp() "
	rm $PARALLEL_FILE_DIR/Parallel_$REFID.txt
	rm $BODY_PATH_REC
	rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
	rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out
	sub="Data Ingestion failed for Dataset $DATASET and table name $TABLENAME"
	echo "Data Ingestion failed for Dataset $DATASET and table name $TABLENAME in script load_dataset.sh" >>$BODY_PATH_REC
	echo "Date:" `date +%Y-%m-%d' '%H:%M:%S:%3N` >>$BODY_PATH_REC

	echo "$to $sub $BODY_PATH_REC $DATASET $TABLENAME"
	#sh $SCRIPTS_PATH/mail_alert_script.sh $to "$sub" $BODY_PATH_REC $DATASET $TABLENAME $SQL_OUT_PATH
	
	exit 1
else
	hive -e "insert into table $DB.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'Completed',current_timestamp() "
	rm $PARALLEL_FILE_DIR/Parallel_$REFID.txt
	rm $BODY_PATH_REC
	rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
	rm $SQL_OUT_PATH/input_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out
	sub="Data Ingestion is success for Dataset $DATASET and table name $TABLENAME"
	echo "Data Ingestion is success for Dataset $DATASET and table name $TABLENAME" >>$BODY_PATH_REC
	echo "Date:" `date +%Y-%m-%d' '%H:%M:%S:%3N` >>$BODY_PATH_REC
	#sh $SCRIPTS_PATH/mail_alert_script.sh $to "$sub" $BODY_PATH_REC $DATASET $TABLENAME $SQL_OUT_PATH
    sh $SCRIPTS_PATH/metavalidation.sh "$SCRIPTS_PATH/common-functions.sh" "$TABLENAME" "$REFID" &	
fi
exit 0
