#*************************************************
#This is the initial script which is triggered to 
#load the script  :this script takes paremeters 
#from the command line arguements
#Author : Abhishek N L - abhishekl@deloitte.com
#*************************************************

#!/bin/bash

#reading the parameters from the commnd line arguements 
tab_name=$1
data_year=$2
stg_db=$3
final_db=$4
dataset=$5
log_file=$6

TABLELOCK="/home/ec2-user/DI_Hive/table_lock"



#assigning the parameter values to the variables
tab_name="${tab_name^^}" 		 #table name to which the data needs to be loaded
stg_db="${stg_db^^}"
final_db="${final_db^^}"
#md_db_name="IKU_DI_REF_METADATA"
#audit_db="IKU_DI_REF_AUDIT"
#audit_table="AUDIT_JOB_RUN_LOG"

CONF_FILE=$7
. $CONF_FILE
data_location=$8
#date_format=`date  +'%m-%d-%Y'`
load_type=$9
delimiter=${10}
TABLEID=${11}
HEADER_COUNT=${12}
POLLTIME=${13}
dataset=${14}
CUR_TABLE_NAME=${15}
TABLENAME=$tab_name
FOOTER_COUNT=${16}

echo $tab_name "$data_location" $data_year $md_db_name $stg_db $final_db $dataset $load_type $delimiter "$TABLEID" "$HEADER_COUNT" "$POLLTIME" $CONF_FILE "$CUR_TABLE_NAME" "$FOOTER_COUNT"
#calling the SQL generate script

sh $SCRIPTS_PATH/generate_script.sh $tab_name "$data_location" $data_year $md_db_name $stg_db $final_db $dataset $load_type $delimiter "$TABLEID" "$HEADER_COUNT" "$POLLTIME" $CONF_FILE "$CUR_TABLE_NAME" "$FOOTER_COUNT"
#need to put a script execution check 


#getting the list of tables in final db into a file
echo "use "$final_db";" > $SQL_OUT_PATH/table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
echo "show tables;" >> $SQL_OUT_PATH/table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql
hive -f $SQL_OUT_PATH/table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out
rm $SQL_OUT_PATH/table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.sql

#creating the external table 
echo " `date  +'%m-%d-%Y %r'` Creating the external table : $tab_name in the database : $stg_db " >> $log_file
hive -f $SQL_OUT_PATH/table_create_external_${dataset}_${TABLENAME}_${POLLTIME}.sql >  $SQL_OUT_PATH/table_create_external_${dataset}_${TABLENAME}_${POLLTIME}.out 
`sleep 30`

rm $SQL_OUT_PATH/table_create_external_${dataset}_${TABLENAME}_${POLLTIME}.out
echo " `date  +'%m-%d-%Y %r'` External table : $tab_name created successfully in the database : $stg_db " >> $log_file


#verifying whether the table name received in parameter is present or not .
table_presence=`grep -i $SQL_OUT_PATH/$tab_name table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out`
table_presence="${table_presence^^}"
rm $SQL_OUT_PATH/table_list_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out
if [ $table_presence == $tab_name ]
then 
	echo " `date  +'%m-%d-%Y %r'` Creating the final managed table : $tab_name  Database: $final_db " >> $log_file          ??????
	#call the CREATE final table and data laod script - insert SQL to load data from external table to interal table
	hive -f $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.out
	echo " `date  +'%m-%d-%Y %r'` Final managed table : $tab_name created successfully in the Database: $final_db " >> $log_file
	rm $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.out
	echo " `date  +'%m-%d-%Y %r'` Data Loading for the table: **$tab_name** has been started in the database : $final_db " >> $log_file
	hive -f $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/table_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out  
else
	#as table is not presentfirst we create the final table 
	echo " `date  +'%m-%d-%Y %r'` Creating the final managed table : $tab_name  Database: $final_db " >> $log_file          ??????
	#call the CREATE final table and data laod script - insert SQL to load data from external table to interal table
	hive -f $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.out
	echo " `date  +'%m-%d-%Y %r'` Final managed table : $tab_name created successfully in the Database: $final_db " >> $log_file
	#rm $SQL_OUT_PATH/table_create_final_${dataset}_${TABLENAME}_${POLLTIME}.out
	echo " `date  +'%m-%d-%Y %r'` Data Loading for the table: **$tab_name** has been started in the database : $final_db " >> $log_file
	hive -f $SQL_OUT_PATH/table_insert_stmt1_${dataset}_${TABLENAME}_${POLLTIME}.sql > $SQL_OUT_PATH/table_insert_stmt_${dataset}_${TABLENAME}_${POLLTIME}.out 
fi

rm $TABLELOCK/$CUR_TABLE_NAME.run

echo " `date  +'%m-%d-%Y %r'` Data loading is complete for the table: $tab_name in database : $final_db" >> $log_file
echo " " >> $log_file
exit 0
