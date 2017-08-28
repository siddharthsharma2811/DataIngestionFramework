#*****************************************************
#This script is used to load the entire data set data
#into the Impala 
#Author : Abhishek N L abhishekl@deloitte.com
#*****************************************************

#!/bin/bash

#input parameter is the data set name
dataset_lower=$1
dataset="${dataset_lower^^}"
#metadata database
#md_db_name="IKU_DI_REF_METADATA"
#curated_s3bucket="cgiku002-interim-sandbox-bucket"
#security="Confidential"
CONF_FILE=$2
. $CONF_FILE
DATA_LOCATION=$3
TABLENAME=$4
DELIMITER=$5
POLLTIME=$6
PIG_LOCATION=$7
S3_LOCATION=$8
echo "jar_file = '$DELIMITER', ${DELIMITER^^}" > $SQL_OUT_PATH/TEST_${dataset}_${TABLENAME}_${POLLTIME}.param
###############Invocation of Pig script in case of No Delimiter Files#########################

if [ "${DELIMITER^^}" == "NO DELIMITER" ] || [ "${DELIMITER^^}" == "NO DEMILITER" ]; then
    
	PIGOUTDIR=${PIG_LOCATION}/${dataset}_${TABLENAME}"_DELIMITED_"$POLLTIME
	
	#aws s3 rm $S3_LOCATION --recursive
	hive -S -e "SELECT cd.column_name,cd.column_position,td.HEADER_COUNT,cd.TABLEID,cd.column_id FROM $md_db_name.column_definition cd join $md_db_name.table_definition td on cd.tableid=td.tableid where td.raw_table_name='$TABLENAME' and td.table_dataset='$dataset' order by cd.TABLEID,cd.column_id;" | sed 's/[\t]/|/g' > $SQL_OUT_PATH/nd_qout_${dataset}_${TABLENAME}_${POLLTIME}.txt
	i=0
	IFS='|'
	while read COLNAME COLPOS HEADER_COUNT X Y Z
	do
		i=`expr $i + 1`
		if [ $i -eq 1 ]; then
             pos_parm=$COLPOS
             field_list=$COLNAME": CHARARRAY"
        else
             pos_parm=$pos_parm", "$COLPOS
             field_list=$field_list", "$COLNAME": CHARARRAY"
        fi
		hdr_cnt=$HEADER_COUNT
	done <$SQL_OUT_PATH/nd_qout_${dataset}_${TABLENAME}_${POLLTIME}.txt
	
	
	
	echo "jar_file = '$PIGGYJAR'" > $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	echo "input_dir = '$PIG_LOCATION'" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	echo "pos_param = '$pos_parm'" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	if [ "hdr_cnt" == "1" ]; then
		header_option="SKIP_HEADER"
		echo "header_option = '$header_option'" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	else
		echo "header_option = ''" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	fi
	
	echo "field_list = '$field_list'" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	echo "output_dir = '$PIGOUTDIR'" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	echo "output_delim = '|'" >> $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param
	
	pig -m $SQL_OUT_PATH/dynmParam_${dataset}_${TABLENAME}_${POLLTIME}.param -f $SCRIPTS_PATH/delim.pig 1>piglog_${dataset}_${TABLENAME}_${POLLTIME}.out 2>piglog_${dataset}_${TABLENAME}_${POLLTIME}.log
	DELIMITER="pipe"
	DATA_LOCATION=$PIGOUTDIR
fi



##############################################################################################

#log file 
date_format=`date  +'%m-%d-%Y_%T'`
log_file="data_load_"${dataset}_$date_format.log  
log_file_path="/home/celgene/data_ingestion/log/"
log="${log_file_path}${log_file}"
echo "Log file :$log"
echo " *********************************************************************" >$log
echo " `date  +'%m-%d-%Y %r'` Data load for the $dataset dataset is started " >>$log
echo " *********************************************************************" >>$log

#retriving the database names from the dataset metadata table
echo "SELECT DISTINCT STAGE_DB from "$md_db_name"."dataset_metadata" where dataset=\""$dataset"\";" > $SQL_OUT_PATH/stg_db_${dataset}_${TABLENAME}_${POLLTIME}.sql
hive -S -f $SQL_OUT_PATH/stg_db_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/stg_db_${dataset}_${TABLENAME}_${POLLTIME}.out
stg_db=`cat $SQL_OUT_PATH/stg_db_${dataset}_${TABLENAME}_${POLLTIME}.out`


echo "SELECT DISTINCT FINAL_DB from "$md_db_name"."dataset_metadata" where dataset=\""$dataset"\";" > $SQL_OUT_PATH/final_db_${dataset}_${TABLENAME}_${POLLTIME}.sql
hive -S -f $SQL_OUT_PATH/final_db_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/final_db_${dataset}_${TABLENAME}_${POLLTIME}.out
final_db=`cat $SQL_OUT_PATH/final_db_${dataset}_${TABLENAME}_${POLLTIME}.out`
#rm $SQL_OUT_PATH/final_db_${dataset}_${TABLENAME}_${POLLTIME}.out $SQL_OUT_PATH/stg_db_${dataset}_${TABLENAME}_${POLLTIME}.out $SQL_OUT_PATH/stg_db_${dataset}_${TABLENAME}_${POLLTIME}.sql $SQL_OUT_PATH/final_db_${dataset}_${TABLENAME}_${POLLTIME}.sql

#retreivign the tables list of tables in the dataset into a file
echo " create database IF NOT EXISTS $stg_db COMMENT 'Database with external tables for the data set $dataset' ;create database IF NOT EXISTS $final_db COMMENT 'Curated database for the data set $dataset' ;" >$SQL_OUT_PATH/tables_script_${dataset}_${TABLENAME}_${POLLTIME}.sql
#echo "SELECT \"sh load_init_parameters.sh \",table_name,datafile_location , data_year,stage_db,final_db,dataset,\"$log\",concat(\"1>\",table_name,\"_${dataset}_${TABLENAME}_${POLLTIME}.out\"),concat(\"2>\",table_name,\".log\") from $md_db_name.dataset_metadata where dataset=\"$dataset\" " >> tables_script_${dataset}_${TABLENAME}_${POLLTIME}.sql


hive -S -e " create database IF NOT EXISTS $stg_db COMMENT 'Database with external tables for the data set $dataset' ;create database IF NOT EXISTS $final_db COMMENT 'Curated database for the data set $dataset' ;" | sed 's/[\t]/|/g' >  $SQL_OUT_PATH/tables_script_${dataset}_${TABLENAME}_${POLLTIME}.out 



if [ $? != 0 ]
then
	echo "Failed in executing the create database script "
	echo "Failed in executing the create database script " >> $log
	exit 1 
fi

echo " `date  +'%m-%d-%Y %r'` Database "$stg_db" and "$final_db" were created sucessfully " >>$log
#loading the data for all tables present in this data set 
#sh tables_script_${dataset}_${TABLENAME}_${POLLTIME}.out
echo "SELECT data_year,stage_db,final_db,dataset,load_type,TABLEID, TABLE_DELIMITER, HEADER_COUNT,FOOTER_COUNT from $md_db_name.dataset_metadata a INNER JOIN $md_db_name.table_definition b ON a.table_name=b.table_name and a.dataset=b.table_dataset where a.dataset='$dataset'  and b.raw_table_name='$TABLENAME' order by tableid ; " >$SQL_OUT_PATH/tables_script_${dataset}_${TABLENAME}_${POLLTIME}.sql
#\"$log\",concat(\"1>\",table_name,\"_${dataset}_${TABLENAME}_${POLLTIME}.out\"),concat(\"2>\",table_name,\".log\")
hive -S -e "SELECT data_year,stage_db,final_db,dataset,load_type,TABLEID, TABLE_DELIMITER, HEADER_COUNT, FOOTER_COUNT, b.TABLE_NAME from $md_db_name.dataset_metadata a INNER JOIN $md_db_name.table_definition b ON a.table_name=b.table_name and a.dataset=b.table_dataset where a.dataset='$dataset'  and b.raw_table_name='$TABLENAME' order by tableid ; " | sed 's/[\t]/|/g' >  $SQL_OUT_PATH/tables_script_${dataset}_${TABLENAME}_${POLLTIME}.out
if [ $? != 0 ]
then
	echo "Failed in executing the dataset metadata script "
	echo "Failed in executing the dataset metadata script " >> $log
	exit 1 
fi
echo "###############"
cat $SQL_OUT_PATH/tables_script_${dataset}_${TABLENAME}_${POLLTIME}.out
echo "###############"
IFS="|"
while read DATA_YEAR STAGE_DB FINAL_DB DATASET LOADTYPE TABLEID TABLE_DELIMITER HEADER_COUNT FOOTER_COUNT CUR_TABLE_NAME X
do
echo $TABLENAME 
	if [ "$HEADER_COUNT" == "" ] || [ "$HEADER_COUNT" == "NULL" ]; then
	HEADER_COUNT=1
	fi
	if [ "$LOADTYPE" == "" ] || [ "$LOADTYPE" == "NULL" ]; then
	LOADTYPE="TRUNCATE_LOAD"
	fi

	sh $SCRIPTS_PATH/load_init_parameters.sh "$TABLENAME" "$DATA_YEAR" "$STAGE_DB" "$FINAL_DB" "$DATASET" "$log" "$CONF_FILE" "$DATA_LOCATION" "$LOADTYPE" "$DELIMITER" "$TABLEID" "$HEADER_COUNT" "$POLLTIME" "$dataset" "$CUR_TABLE_NAME" "$FOOTER_COUNT" ""1>$SQL_OUT_PATH/${dataset}_${TABLENAME}_${POLLTIME}.out 2>$SQL_OUT_PATH/${dataset}_${TABLENAME}_${POLLTIME}.log
	
	if [ $? != 0 ]
	then
        echo "error while running load_init_parameters" >> $log
        exit 2
	fi
done <$SQL_OUT_PATH/tables_script_${dataset}_${TABLENAME}_${POLLTIME}.out


echo " *********************************************************************" >>$log
echo " `date  +'%m-%d-%Y %r'` Data load for the $dataset dataset is complete " >>$log
echo " *********************************************************************" >>$log

exit 0


