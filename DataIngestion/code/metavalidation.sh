#!/bin/bash

echo "Executing Started"
source $1

tabname=$2
refid_input=$3
#sas_extract=$4


logfilename="execution.log"
impalaqueryfilename=$home_dir/scripts/extract.sql
impalaoutputfile=$home_dir/temp/impalaextract$refid_input
logfilefullpath=$log"/"$logfilename
raw_table_output=$log/raw_table.out_$refid_input
raw_table_location=$log/raw_table_location.out_$refid_input
scriptname=`basename "$0"`


#Get the details from metadata tables
query="SELECT TEMP.column_id,TEMP.tableid,TEMP.destination_path,TEMP.TABLE_NAME,TEMP.raw_table_name,upper(TEMP.delimiter),TEMP.unique_id,TEMP.header_count,    TEMP.column_position,dm.final_db,TEMP.dataset_id,dm.stage_db,TEMP.dataset,dm.sas_dataset FROM (SELECT dt2.destination_path,dt2.TABLE_NAME,dt2.unique_id,td.raw_table_name,td.tableid,dt2.delimiter,td.header_count,cd.column_id,cd.column_position,dds.dataset,dt2.dataset_id FROM datalakepoc_metadata.dt2analyzer dt2 JOIN datalakepoc_metadata.dataset_definition_static dds ON dt2.dataset_id = dds.sourceid JOIN datalakepoc_metadata.table_definition td ON td.table_name = dt2.table_name AND td.table_dataset = dds.dataset JOIN (SELECT a.column_id,a.tableid, a.column_position FROM datalakepoc_metadata.column_definition a INNER JOIN (SELECT tableid,max(column_id) AS max_count FROM datalakepoc_metadata.column_definition WHERE upper(column_name) <> \"ORACLE_ID\" GROUP BY tableid) b ON a.tableid = b.tableid AND a.column_id = b.max_count) cd ON cd.tableid = td.tableid) TEMP JOIN datalakepoc_metadata.dataset_metadata dm ON TEMP.TABLE_NAME = dm.table_name AND dm.dataset = TEMP.dataset AND TEMP.TABLE_NAME=\"$tabname\" and TEMP.unique_id=$refid_input;"
fnExecImpalaShell q "\${query}" "\${impalaoutputfile}" "\${logfile}"

echo "Impala output loc:$impalaoutputfile"
echo `cat $impalaoutputfile`
temp=$(wc -c $impalaoutputfile)
bytecount=$(echo $temp |cut -d' ' -f1)


if [ "$bytecount" -eq 1 ]
then
		syntax=_%
		new_tabname="$tabname$syntax"
		query="SELECT TEMP.column_id,TEMP.tableid,TEMP.destination_path,TEMP.TABLE_NAME,TEMP.raw_table_name,upper(TEMP.delimiter),TEMP.unique_id,TEMP.header_count,    TEMP.column_position,dm.final_db,TEMP.dataset_id,dm.stage_db,TEMP.dataset,dm.sas_dataset FROM (SELECT dt2.destination_path,td.TABLE_NAME,dt2.unique_id,td.raw_table_name,td.tableid,dt2.delimiter,td.header_count,cd.column_id,cd.column_position,dds.dataset,dt2.dataset_id FROM datalakepoc_metadata.dt2analyzer dt2 JOIN datalakepoc_metadata.dataset_definition_static dds ON dt2.dataset_id = dds.sourceid JOIN datalakepoc_metadata.table_definition td ON td.raw_table_name = dt2.table_name AND td.table_dataset = dds.dataset JOIN (SELECT a.column_id,a.tableid, a.column_position FROM datalakepoc_metadata.column_definition a INNER JOIN (SELECT tableid,max(column_id) AS max_count FROM datalakepoc_metadata.column_definition WHERE upper(column_name) <> \"ORACLE_ID\" GROUP BY tableid) b ON a.tableid = b.tableid AND a.column_id = b.max_count) cd ON cd.tableid = td.tableid) TEMP JOIN datalakepoc_metadata.dataset_metadata dm ON TEMP.TABLE_NAME = dm.table_name AND dm.dataset = TEMP.dataset AND TEMP.raw_TABLE_NAME like \"$new_tabname\" and TEMP.unique_id=$refid_input";
		
		fnExecImpalaShell q "\${query}" "\${impalaoutputfile}" "\${logfile}"
		temp=$(wc -c $impalaoutputfile)
		bytecount=$(echo $temp |cut -d' ' -f1)
			if [ "$bytecount" -eq 1 ] 
			then
			writelog $logfilefullpath "ERROR" $scriptname "empty extract file , Please investigate"
			exit 1
			fi
fi


line=$(head -1 $impalaoutputfile)
IFS='|' read -r -a array <<< "$line"

## Format After Split
## Location 0 : Number of columns in table
## Location 1 : Table ID
## Location 2 : Destination Path
## Location 3 : Table Name
## Location 4 : Raw Table Name
## Location 5 : Table Delimiter ( Please convert the string into upper Case)
## Location 6 : Unique ID
## Location 7 : Head_count --> Which states the file has header or not
## Location 8 : Column Position for No Delimiter File
## Location 9 : Database name
		## Location 10 : dataset id

		metadata_no_of_columns=${array[0]}
		metadata_table_id=${array[1]}
		metadata_destination_path=${array[2]}
		metadata_table_name=${array[3]}
		metadata_raw_table_name=${array[4]}
		metadata_table_delimiter=${array[5]}
		metadata_unique_id=${array[6]}
		metadata_header_present_flag=${array[7]}
		metadata_column_position=${array[8]}
		metadata_database_name=${array[9]}
		metadata_dataset_id=${array[10]}
		metadata_stage_database_name=${array[11]}
		metadata_dataset_name=${array[12]}
		metadata_sas_dataset=${array[13]}
		header_present="FALSE"
		
				
#----------------Header Check
	metadata_header_record_temp=$(hive -k -i $parImpServer:$parImpPort --ssl --ca_cert=/opt/cloudera/security/pki/impala.pem --var=tablevar=$metadata_table_id-e ' select column_name from datalakepoc_metadata.column_definition where tableid= $metadata_table_id and upper(column_name) <> "ORACLE_ID" ORDER BY column_id ;')
        echo "THE METAHEADER RECCORD $metadata_header_record_temp"
	java_header_match="$(java -jar $jarname MatchHeader "$metadata_header_record_temp" "$firstrec" "COMMA")"
	
	 writelog $logfilefullpath "INFO" $scriptname "Header Match : $java_header_match"
	if [ "$java_header_match" == "true" ]
					then
									header_present="TRUE"
	fi
#----------------Header Check
		

#---------------- FILERECORDCOUNT		
			metadata_destination_path=`echo $metadata_destination_path | sed -e "s/s3a:/s3:/g"`
			echo $metadata_destination_path
			#filename=$(basename "${metadata_destination_path}")
			filename=`aws s3 ls ${metadata_destination_path} | rev | cut -d" " -f1 | rev`
			filename=`echo $filename | grep -v '^$'`
			echo "the filename is $filename"
			metadata_destination_path=${metadata_destination_path}${filename}
			fnAWSCpToLcl "${metadata_destination_path}" $logfilefullpath $parTmpLoc         
		 
			#Check for empty file and trigger email
			empty_check_temp=$(wc -c $parTmpLoc/$filename)
			empty_check_count=$(echo $empty_check_temp |cut -d' ' -f1)

			if [ "$empty_check_count" -lt 1 ]
			then
				fnSendNotification "EmptyFile" "The-file-is-empty" "$parTmpLoc/$filename" "logfilefullpath"
				writelog $logfilefullpath "ERROR" $scriptname "empty extract file , Please investigate"
			fi
#---------------- FILERECORDCOUNT
	

#----------------File Section
echo "File Section"

	firstrec="$(head -1 $parTmpLoc/$filename)"
	firstrec=`echo "$firstrec" | tr -d '" "'`
	firstrec=`echo $firstrec | sed -e "s/;/,/g"`
	echo "The new firstrect is $firstrec"
	recordminlenth=`echo ${metadata_column_position} | cut -d'-' -f1`

#----------------Column Check
	fnFetchFileColumnCount "${metadata_unique_id}" "${metadata_dataset_id}" "${metadata_destination_path}" "${metadata_table_name}" "COMMA" "${firstrec}" "${recordminlenth}" "FILECOLUMNCOUNT" $metadata_no_of_columns 
#----------------Column Check



#----------------DatatoColumn Mapping Check
				filelocation="$parTmpLoc/$filename"
                
                secondrec=`sed -n '2p' $filelocation`
                

		fnDataTypeComparision  "${metadata_table_id}" "${logfilefullpath}" "${parDbname}" "${Column_Defination_Table}" "${metadata_table_delimiter}" "${secondrec}" "${metadata_unique_id}" "DATATOSCHEMACHECK" "${metadata_dataset_id}" "${metadata_table_name}"
		
#----------------DatatoColumn Mapping Check	

#----------------File Record Count

	fnLogValidRecCnt f $metadata_unique_id $metadata_dataset_id "${parTmpLoc}" "${filename}" $logfilefullpath "FILERECORDCOUNT" $header_present $metadata_raw_table_name

#----------------File Record Count

#*****************File Section

#----------------Impala Section
echo "Impala Section Section"

	#----------------Impala External table location check
		#fnCheckTableLocation "${metadata_unique_id}" "${metadata_stage_database_name}" "${metadata_raw_table_name}" "${metadata_destination_path}" "${logfilefullpath}" "${metadata_dataset_id}" "EXTERNALTABLOC" "${metadata_dataset_name}" "${DT2_Analyzer_Status}"
	#----------------Impala Record count
		outfile_imp=$parTmpLoc/$filename"impala_out"
		query="select count(*) from $metadata_database_name.$metadata_table_name"
		fnExecImpalaShell q "\${query}" "\${outfile_imp}" "\${logfile}"

		impcount=$(head -1 $outfile_imp)
		query="insert into $parDbname.$parAudTblName select $metadata_unique_id,\"$metadata_dataset_id\",\"$metadata_destination_path\",\"$metadata_table_name\",\"IMPALARECCOUNT\",\"$impcount\",current_timestamp()"
		fnExecImpalaShell q "\${query}" "\${outfile_imp}" "\${logfile}"

	#----------------Impala Column count
		query="insert into $parDbname.$parAudTblName select $metadata_unique_id,\"$metadata_dataset_id\",\"\",\"$metadata_table_name\",\"IMPALACOLUMNCOUNT\",\"$metadata_no_of_columns\",current_timestamp()"
		fnExecImpalaShell q "\${query}" "\${outfile_imp}" "\${logfile}"

#***************** Impala Section


#----------------Validation Section
echo "Validation"
	sleep 30
	fnChkValidMatch $metadata_unique_id "FILERECORDCOUNT" "IMPALARECCOUNT" $logfilefullpath 
	fnChkValidMatch $metadata_unique_id "FILECOLUMNCOUNT" "IMPALACOLUMNCOUNT" $logfilefullpath
#*****************Validation Sectin




#----------------Final Status
echo "Final Status"
  
	outfile_imp=$parTmpLoc/$metadata_unique_id"finalStatus"
	query="select count(1) from $auddbname.$parResTblName where refid=$metadata_unique_id and trim(upper(status))=\"FAILURE\" ;"
	fnExecImpalaShell q "\${query}" "\${outfile_imp}" "\${logfile}"
	outfile_imp_cnt=$(head -1 $outfile_imp)

	if [ "$outfile_imp_cnt" -eq 0 ]
			then
					query="insert into "$auddbname"."$parResTblName" select $refid,\"FINALRESULT\",\"\",\"\",\"PASS\",current_timestamp();"
					fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
	else
					query="insert into "$auddbname"."$parResTblName" select $refid,\"FINALRESULT\",\"\",\"\",\"FAIL\",current_timestamp();"
					fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
	fi

#*****************Final Status

	#remove the files copied
	rm $parTmpLoc/$filename
	rm $outfile_imp
			

exit 0
