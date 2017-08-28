#!/bin/bash
#   Data Ingestion Automation Event Handler Script
#	Event handler to identify new request from Data-Transport API and trigger DI
#	Author: Siddharth Sharma     
#		                                              
#       $1 = properties file name passed with parameters   

# Poll the DT2Analyzer table to identify a new request
#md_db_name="iku_di_ref_metadata"
#PARALLEL_FILE_DIR="/home/celgene/data_ingestion/scripts/parallelExec"
#SCRIPTS_PATH="/home/celgene/data_ingestion/scripts/"
#NB_START_TIME=170000
#NB_END_TIME=050000
#NO_OF_PARALLEL_RUN=5
CONF_FILE=$1
. $CONF_FILE
DT=`date +%Y%m%d`
NB_S_TIMESTAMP="$DT$NB_START_TIME"
NB_E_TIMESTAMP="$DT$NB_END_TIME"
POLLTIME=`date +%Y%m%d%H%M%S`
load_id=$POLLTIME

if [ ! -d "$PARALLEL_FILE_DIR" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir -p $PARALLEL_FILE_DIR
fi


if [ ! -d "$SQL_OUT_PATH" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir -p $SQL_OUT_PATH
  
fi

hive -S -e  "select d2a.unique_id,d2a.dataset_id,d2a.destination_path, d2a.processing_priority, d2a.table_name, d2a.delimiter from $md_db_name.dt2analyzer d2a left outer join $md_db_name.dt2analyzer_status d2as on d2a.unique_id=d2as.refid and d2a.dataset_id=d2as.dataset_id and upper(d2a.table_name)=upper(d2as.table_name) and d2as.status = \"Received\" where d2as.refid is null order by d2a.unique_id" | sed 's/[\t]/|/g' > $SQL_OUT_PATH/dt2analyzer_mail_$POLLTIME.out 

IFS='|'
while read REFID DATASOURCEID DATALOCATION PRIORITYFLAG TABLENAME DELIMITER X
do
	TABLENAME=${TABLENAME^^}
	BODY_PATH_REC=${BODY_PATH}/${DATASOURCEID}_${TABLENAME}_${POLLTIME}'_body'
	REFID=`echo $REFID|tr -s " "`
	load_id=$REFID
	if [ "$REFID" == "" ]; then
		break
	fi
	
	echo "triggering mail request for ref ID: $REFID"
	sub="Data Ingestion request received for Data Source ID $DATASOURCEID and table name $TABLENAME"
	rm $BODY_PATH_REC
	echo "Data Ingestion request received for Data Source ID $DATASOURCEID and table name $TABLENAME with PRIORITYFLAG $PRIORITYFLAG" >>$BODY_PATH_REC
	echo "Date:" `date +%Y-%m-%d' '%H:%M:%S:%3N` >>$BODY_PATH_REC
	
	echo "$to $sub $BODY_PATH_REC $DATASOURCEID $TABLENAME"
	sh $SCRIPTS_PATH/mail_alert_script.sh $to "$sub" $BODY_PATH_REC $DATASOURCEID $TABLENAME $SQL_OUT_PATH &
	
	hive -e "insert into table $md_db_name.dt2analyzer_status(refid,dataset_id,table_name,status,rec_insert_date) select $REFID,\"$DATASOURCEID\",\"$TABLENAME\",'Received',current_timestamp() "

done <$SQL_OUT_PATH/dt2analyzer_mail_$POLLTIME.out

rm $RUNFILE/*.ack
exit 0
