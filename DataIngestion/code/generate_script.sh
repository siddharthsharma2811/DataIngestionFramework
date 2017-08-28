#*****************************************************
#This script is used to generate the SQL scripts 
#used for data load for the table passed as parameter 
#Author : Abhishek N L abhishekl@deloitte.com
#*****************************************************

#!/bin/bash

#defining the variables requried 
tab_name=$1
data_file_loc=$2
data_year=$3
#database details 
#md_db_name="IKU_DI_REF_METADATA" #db where metadata is stored 
#stg_db="STG_TRUVEN" 		#db where external tables are created
#final_db="FINAL_TRUVEN"		#db where final internal tables are created

md_db_name=$4
stg_db=$5
final_db=$6
dataset=$7
load_type=$8
delimiter=$9
TABLEID=${10}

HEADER_COUNT=${11}
POLLTIME=${12}
CONF_FILE=${13}
. $CONF_FILE
TABLENAME=$tab_name
CUR_TABLE_NAME=${14}
FOOTER_COUNT=${15}
TABLELOCK="${E2EPATH}/table_lock"
#data_location=$8

#retreving the tableid for the table name passed as parameter 
#tabid_stmt="SELECT TABLEID FROM "$md_db_name".table_definition where table_name=\""$tab_name"\" and table_dataset=\""$dataset"\";"
#echo $tabid_stmt > temp_tabid_${dataset}_${TABLENAME}_${POLLTIME}.sql
#hive -k -i z9awsspiku2e107.celgene.com:25003 -S-f temp_tabid_${dataset}_${TABLENAME}_${POLLTIME}.sql -o temp_tabid_$tab_name
#
#tableid=`cat temp_tabid_$tab_name`
#rm temp_tabid_$tab_name*_${dataset}_${TABLENAME}_${POLLTIME}.sql
#rm temp_tabid_$tab_name
#
##retriving the table delimiter
#tab_delim="SELECT TABLE_DELIMITER FROM "$md_db_name".table_definition where table_name=\""$tab_name"\" and table_dataset=\""$dataset"\";"
#echo $tab_delim > tab_delim_${dataset}_${TABLENAME}_${POLLTIME}.sql
#hive -k -i z9awsspiku2e107.celgene.com:25003 -S-f tab_delim_${dataset}_${TABLENAME}_${POLLTIME}.sql -o tab_delim_${dataset}_${TABLENAME}_${POLLTIME}.out
#
#table_delim=`cat tab_delim_${dataset}_${TABLENAME}_${POLLTIME}.out`
#rm tab_delim_${dataset}_${TABLENAME}_${POLLTIME}.out tab_delim_${dataset}_${TABLENAME}_${POLLTIME}.sql

#retreving the tableid, delimiter and header count for the table name passed as parameter 
#hive -k -i z9awsspiku2e107.celgene.com:25003 -S --output_delimiter='|'-e "SELECT TABLEID, TABLE_DELIMITER, HEADER_COUNT FROM $md_db_name.table_definition where table_name='$tab_name' and table_dataset='$dataset'" -o "$tab_name"_qr_${dataset}_${TABLENAME}_${POLLTIME}.out

#if [ $? -eq 0 ]
#then
#	IFS='|'
#	CNT=0
#	echo "going to while loop"
#	while read TABLEID TABLE_DELIM TAB_HEADERCNT X
#	do
#		tableid=`echo $TABLEID`
#		table_delim=`echo $TABLE_DELIM`
#		tab_headercnt=`echo $TAB_HEADERCNT`
#		echo " tableid: $tableid , tab_delim: $table_delim, tab_headercnt: $tab_headercnt"
#		CNT=`expr $CNT + 1`
#		tab_headercnt=`echo $tab_headercnt|tr -s " "`
#		if [ "$tab_headercnt"=='NULL' ]
#		then
#			tab_headercnt=1
#		fi
#		echo " tableid: $tableid , tab_delim: $table_delim, tab_headercnt: $tab_headercnt"
#	
#	done < "$tab_name"_qr_${dataset}_${TABLENAME}_${POLLTIME}.out
	tableid=$TABLEID
	tab_headercnt=$HEADER_COUNT
	delimiter=${delimiter^^}
	
	if [ "$delimiter" == 'PIPE' ]
	then
		tab_delim="|"
	elif [ "$delimiter" == 'COMMA' ]
	then
		tab_delim=","
	elif [ "$delimiter" == 'TAB' ]
	then
		tab_delim="\t"
	elif [ "$delimiter" == 'SPACE' ]
	then
		tab_delim=" "
	elif [ "$delimiter" == 'SEMICOLON' ]
	then
		tab_delim=";"
	else
                tab_delim=$delimiter
		echo "delimiter not identified"
	fi
		
	#if [ $CNT -gt 1 ]
	#then
	#	echo "Number of rows for dataset: $dataset and tablename: $tab_name combination is more than 1"
	#	exit 1
	#fi

#else
#	echo "impala query to get delimiter, header count and tableid failed."
#	exit 1
#fi



#storing number of columns into a variable
column_cnt_stmt="SELECT COUNT(COLUMN_NAME) FROM "$md_db_name".COLUMN_DEFINITION WHERE TABLEID="$tableid" ;"
echo $column_cnt_stmt > $SQL_OUT_PATH/temp_clm_cnt_${dataset}_${TABLENAME}_${POLLTIME}.sql
hive -S -f $SQL_OUT_PATH/temp_clm_cnt_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/temp_clm_cnt_${dataset}_${TABLENAME}_${POLLTIME}.out

#variable stores the column count of a table
column_cnt=`cat $SQL_OUT_PATH/temp_clm_cnt_${dataset}_${TABLENAME}_${POLLTIME}.out`
rm $SQL_OUT_PATH/temp_clm_cnt_$tab_name*

#retriving the list of column into a file 
column_lst_stmt="SELECT COLUMN_ID,concat('\`', COLUMN_NAME,'\`') as COLUMN_NAME FROM "$md_db_name".COLUMN_DEFINITION WHERE TABLEID="$tableid" ORDER BY COLUMN_ID ;"
echo $column_lst_stmt > $SQL_OUT_PATH/temp_clmn_${dataset}_${TABLENAME}_${POLLTIME}.sql 
hive -S -f $SQL_OUT_PATH/temp_clmn_${dataset}_${TABLENAME}_${POLLTIME}.sql | sed 's/[\t]/|/g'> $SQL_OUT_PATH/table_column_list_1${dataset}_${TABLENAME}_${POLLTIME}.out 
cut -d'|' -f2- $SQL_OUT_PATH/table_column_list_1${dataset}_${TABLENAME}_${POLLTIME}.out > $SQL_OUT_PATH/table_column_list_${dataset}_${TABLENAME}_${POLLTIME}.out

#retriving the column cts datatype and omments into a file : required for creating the internal final table CREATE table script
column_comment_stmt="SELECT COLUMN_ID,concat('\`', COLUMN_NAME,'\`') as COLUMN_NAME ,COLUMN_DATATYPE ,'COMMENT ','\'',COLUMN_DESCRIPTION,'\'' FROM "$md_db_name".COLUMN_DEFINITION WHERE TABLEID="$tableid" ORDER BY COLUMN_ID ;"
echo $column_comment_stmt > $SQL_OUT_PATH/temp_clm_cnt_${dataset}_${TABLENAME}_${POLLTIME}.sql
hive -S -f $SQL_OUT_PATH/temp_clm_cnt_${dataset}_${TABLENAME}_${POLLTIME}.sql | sed 's/[\t]/|/g'  > $SQL_OUT_PATH/table_column_comment_1${dataset}_${TABLENAME}_${POLLTIME}.out
cut -d'|' -f2- $SQL_OUT_PATH/table_column_comment_1${dataset}_${TABLENAME}_${POLLTIME}.out | sed 's/|/\t/g' > $SQL_OUT_PATH/table_column_comment_${dataset}_${TABLENAME}_${POLLTIME}.out

#Generating the CREATE external table script 
count=0
echo "use "$stg_db" ;
DROP TABLE IF EXISTS "$stg_db"."$tab_name " PURGE;
 CREATE EXTERNAL TABLE  "$stg_db"."$tab_name "
( ">$SQL_OUT_PATH/temp_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql

	while read LINE 
	do 
		((count++)) #increase the line counter
		if [ $count -lt $column_cnt ]
		then
			echo $LINE" STRING  ," >>$SQL_OUT_PATH/temp_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql
		else
			echo $LINE" STRING  " >>$SQL_OUT_PATH/temp_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql
		fi		
	done <$SQL_OUT_PATH/table_column_list_${dataset}_${TABLENAME}_${POLLTIME}.out

echo " ) 
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
	WITH SERDEPROPERTIES ('separatorChar'='"$tab_delim"')
	LOCATION '"$data_file_loc"'
	tblproperties (\"skip.header.line.count\"=\"$tab_headercnt\",\"skip.footer.line.count\"=\"$FOOTER_COUNT\"); " >> $SQL_OUT_PATH/temp_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql

mv $SQL_OUT_PATH/temp_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql $SQL_OUT_PATH/table_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql
rm $SQL_OUT_PATH/table_column_list_${dataset}_${TABLENAME}_${POLLTIME}.out $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql

#Generating the internal table CREATE statement 
count1=0
if [ "${load_type^^}" != "INCREMENTAL" ]
then 
	echo "DROP TABLE IF EXISTS $final_db.$CUR_TABLE_NAME PURGE;"> $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql
fi	
echo "use "$final_db" ;
CREATE TABLE IF NOT EXISTS "$final_db"."$CUR_TABLE_NAME"
( " >> $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql
	
	while read LINE 
	do 
		((count1++))
		if [ $count1 -lt $column_cnt ]
		then 
			echo $LINE",">>$SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql
		else
			echo $LINE >> $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql
		fi
	done < $SQL_OUT_PATH/table_column_comment_${dataset}_${TABLENAME}_${POLLTIME}.out
echo " )
PARTITIONED BY(IKU_DATA_YEAR  INT, IKU_SOURCE STRING)
STORED AS PARQUET
;" >> $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql

#rm $SQL_OUT_PATH/table_column_comment_${dataset}_${TABLENAME}_${POLLTIME}.out

#generating insert statement to load from external table to internal table



table_insert_stmt="SELECT COLUMN_ID, CASE lower(COLUMN_DATATYPE) WHEN 'timestamp' then IF ( DATE_FORMAT = 'yyMMdd', concat(\"IF (cast(SUBSTR(\",concat('\`', COLUMN_NAME,'\`'),\",1,2) as bigint) >= \",\"00\",\" AND cast(SUBSTR(\",concat('\`', COLUMN_NAME,'\`'),\",1,2) as bigint) <= \",\"37\", \" ,cast(from_unixtime(unix_timestamp(concat(concat(cast(\",\"20\",\"\ as string),SUBSTR(\",concat('\`', COLUMN_NAME,'\`'),\",1,2)),SUBSTR(\",concat('\`', COLUMN_NAME,'\`'),\",3,4))\",\",\",\"\'\",\"yyyyMMdd\",\"\'\",\")) as \",COLUMN_DATATYPE,\" ), cast(from_unixtime(unix_timestamp(concat(concat(cast(\",\"19\",\"\ as string),SUBSTR(\",concat('\`', COLUMN_NAME,'\`'),\",1,2)),SUBSTR(\",concat('\`', COLUMN_NAME,'\`'),\",3,4))\",\",\",\"\'\",\"yyyyMMdd\",\"\'\",\")) as \",COLUMN_DATATYPE,\" )) as \", concat('\`', COLUMN_NAME,'\`')),concat(\"cast(from_unixtime(unix_timestamp(\",concat('\`', COLUMN_NAME,'\`'),\",\",\"\\'\", DATE_FORMAT,\"\\'\",\")) as \",COLUMN_DATATYPE,\" ) as \",concat('\`', COLUMN_NAME,'\`') )) else concat(\"cast(\",concat('\`', COLUMN_NAME,'\`'),\" as \",COLUMN_DATATYPE,\" ) as \",concat('\`', COLUMN_NAME,'\`') ) END  FROM "$md_db_name".COLUMN_DEFINITION WHERE TABLEID="$tableid" ORDER BY COLUMN_ID ;"

echo $table_insert_stmt > $SQL_OUT_PATH/table_gen_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
hive -S -f $SQL_OUT_PATH/table_gen_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql | sed 's/[\t]/|/g' > $SQL_OUT_PATH/table_insert_stmt1_1${dataset}_${TABLENAME}_${POLLTIME}.out
cut -d'|' -f2- $SQL_OUT_PATH/table_insert_stmt1_1${dataset}_${TABLENAME}_${POLLTIME}.out > $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.out

#rm $SQL_OUT_PATH/table_gen_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql


count2=0
#echo "USE "$final_db" ;
#INSERT INTO TABLE "$final_db"."$CUR_TABLE_NAME"
#PARTITION (IKU_DATA_YEAR="$data_year",IKU_SOURCE='"$dataset"')
#SELECT " > $SQL_OUT_PATH/table_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql

########MUlti File Fix####################

hive -S -e "select raw_table_name from "$md_db_name".TABLE_DEFINITION where table_name='$CUR_TABLE_NAME' and table_dataset='$dataset'" | sed 's/[\t]/|/g'  > $SQL_OUT_PATH/CUR_RAW_TBL_${dataset}_${TABLENAME}_${POLLTIME}.sql



########MUlti File Fix####################
rm $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql

#Locking TABLE
if [ ! -d "$TABLELOCK" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir -p $TABLELOCK
  
fi


#Locking TABLE
IFS='|'
	var=0
	while [ $var -lt 1 ] 
	do
		filecnt=`ls $TABLELOCK/$CUR_TABLE_NAME.run|wc -l`
		if [ $filecnt -gt 0 ]
		then
			echo "One request for this table is already processing, sleeping for 2 mins"
			sleep 2m
			#echo "DI Processing has reached threshold and  "
		else
			var=1
		fi
	
	done 
touch $TABLELOCK/$CUR_TABLE_NAME.run
while read RAW_TABLE_NAME
do
count2=0
		echo "USE "$final_db" ;
		INSERT OVERWRITE TABLE "$final_db"."$CUR_TABLE_NAME"
		PARTITION (IKU_DATA_YEAR="$data_year",IKU_SOURCE='"$dataset"')
		SELECT " >> $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql
	while read LINE
		do

		#"SELECT " >>$SQL_OUT_PATH/table_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
			((count2++))
			if [ $count2 -lt $column_cnt ]
			then 
				echo $LINE" ,"  >>$SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql
			else
				echo $LINE  >>$SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql
			fi
	done < $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.out
	
	echo " FROM "$stg_db"."$RAW_TABLE_NAME";" >>$SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql

done < $SQL_OUT_PATH/CUR_RAW_TBL_${dataset}_${TABLENAME}_${POLLTIME}.sql
#rm  $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.out

exit 0

