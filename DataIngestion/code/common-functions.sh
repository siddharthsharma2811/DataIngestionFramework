#!/bin/bash

#initialize global parameters
#. ../param/gblpa
. /home/ec2-user/data_ingestion/gblparam.prm

writelog()
{
        dt=`date '+%d/%m/%Y_%H:%M:%S'`
        logfile=$1
        logtype=$2
        scriptname=$3
        desc=$4

        echo "[$dt][$logtype][$scriptname][$desc]" >> $logfile
}

fnExecImpalaShell()
{
        impserver=$parImpServer
        impport=$parImpPort
        certificate=$parmCertificateloc
        eval option=$1
        eval queryinput=$2
        eval output=$3
        eval logfile=$4
        scriptname=`basename "$0"`

		rm $output
		touch $output
		
        if [ "$option" == "q" ]
        then
                writelog $logfile INFO $scriptname "Executing an Impala Statement and redirecting output to $output"
                writelog $logfile DETAIL $scriptname "Impala Statement: $queryinput"
                #impala-shell -k -i $impserver:$impport --ssl --ca_cert=$certificate -B --output_delimiter='|' -q "$queryinput" -o $output
		hive -S -e "$queryinput" | sed 's/[\t]/|/g' > $output

                if [ $? -ne 0 ]
                then
                        writelog $logfile ERROR $scriptname "Failure executing statement. Output File:$output. Statement executed:$queryinput"
                        #insert code for alerting here. pending
                        exit 1
                else
                        writelog $logfile INFO $scriptname "Statement executed successfully. Output redirected to $output"
                fi

        elif [ "$option" == "f" ]
        then
                if [ -f $queryinput ]; then
                        writelog $logfile INFO $scriptname "Executing an Impala Script below and redirecting output to $output"
                        cat $queryinput >> $logfile
                        #impala-shell -k -i $impserver:$impport --ssl --ca_cert=$certificate -B --output_delimiter='|' -f $queryinput -o $output
			hive -S -f $queryinput | sed 's/[\t]/|/g' > $output

                        if [ $? -ne 0 ]
                        then
                                writelog $logfile ERROR $scriptname "Failure executing script.$queryinput. Output redirected to $ouput"
                                #insert code for alerting here. pending
                                exit 1
                        else
                                writelog $logfile INFO $scriptname "Impala Script executed successfully. Output redirected to $output"
                        fi
                else
                        writelog $logfile ERROR $scriptname "Failure executing impala script file. File not found : $queryinput"
                        #insert code for alerting here. pending
                        exit 1
                fi
        else
                writelog $logfile ERROR $scriptname "Invalid options passed to function execute impala shell. Use q for query, f for a script file."
        fi

}


fnLogValidRecCnt()
{
        dt=`date '+%d_%m_%Y.%H_%M_%S'`
        auddbname=$parDbname
        audtable=$parAudTblName
        option=$1
        refid=$2
        datasetid=$3
        db_fileloc=$4
        cnttblfilename=$5
        logfile=$6
        chktype=$7
        header_present=TRUE
		raw_table_name=$9
		
        scriptname=`basename "$0"`
        output=$data/$db_fileloc.$cnttblfilename.$dt.out
echo "THE DELIMITER VALUES IS $"		
		

  if [[ "$option" == "f" && "$raw_table_name" == *[0-9]* ]]
        then
		  
                if [ -f $db_fileloc/$cnttblfilename ]
                then
                        reccnt=`awk 'END {print NR}' "$db_fileloc/$cnttblfilename"`
						#return $reccnt
						

                                                if [ "$header_present" == "TRUE" ]
                                                then
                                                        reccnt=$((reccnt-1))
														return $reccnt
							#reccnt=`expr reccnt - 1`
							writelog $logfile INFO $scriptname "Header Match Found"
                                                fi
			
                        writelog $logfile INFO $scriptname "Record count of the file $db_fileloc/$cnttblfilename is $reccnt"
                else
                        writelog $logfile ERROR $scriptname "Error while checking record count. File  $db_fileloc/$cnttblfilename not found"
                fi
		
		elif  [[ "$option" == "f" && "$raw_table_name" != *[0-9]* ]]
        then
		  echo "THIS IS FILE LOCATION IN RECORD COUNT $db_fileloc/$cnttblfilename"
                if [ -f $db_fileloc/$cnttblfilename ]
                then
                        reccnt=`awk 'END {print NR}' "$db_fileloc/$cnttblfilename"`
                             echo "HEADER VALUES RECEIVED:$header_present"
						

                                                if [ "$header_present" == "TRUE" ]
                                                then
                                                        reccnt=$((reccnt-1))
							#reccnt=`expr reccnt - 1`
							writelog $logfile INFO $scriptname "Header Match Found"
                                                fi
			
                        writelog $logfile INFO $scriptname "Record count of the file $db_fileloc/$cnttblfilename is $reccnt"
						
						output=$log/insert_aud_"$cnttblfilename"_dt.out
						query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$db_fileloc"\",\""$cnttblfilename"\",\""$chktype"\",\""$reccnt"\",current_timestamp();"
						fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
						rm -f $output
                else
                        writelog $logfile ERROR $scriptname "Error while checking record count. File  $db_fileloc/$cnttblfilename not found"
                fi
		
        elif  [ "$option" == "d" ]
        then
                query="select count(1) from $db_fileloc.$cnttblfilename"
                fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
                reccnt=$(head -n 1 $output)
				output=$log/insert_aud_"$cnttblfilename"_dt.out
				query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$db_fileloc"\",\""$cnttblfilename"\",\""$chktype"\",\""$reccnt"\",current_timestamp();"
				fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
                rm -f $output
        fi

        

}

fnChkValidMatch()
{
        auddbname=$parDbname
        audtable=$parAudTblName
        refid=$1
        matchsrc=$2
        matchtgt=$3
        logfile=$4
		
        output=$data/validmatch.out$refid
        scriptname=`basename "$0"`
		
        qrymtchsrc="select max(chk_value) from "$auddbname"."$audtable" where refid="$refid" and chk_type=\""$matchsrc"\" and rec_insert_date = (select max(rec_insert_date) from "$auddbname"."$audtable" where refid="$refid" and chk_type=\""$matchsrc"\");"
        qrymtchtgt="select max(chk_value) from "$auddbname"."$audtable" where refid="$refid" and chk_type=\""$matchtgt"\" and rec_insert_date = (select max(rec_insert_date) from "$auddbname"."$audtable" where refid="$refid" and chk_type=\""$matchtgt"\");"
        fnExecImpalaShell q "\${qrymtchsrc}" "\${output}" "\${logfile}"
        sleep 5
		srcmtchval=$(head -n 1 $output)
		
        fnExecImpalaShell q "\${qrymtchtgt}" "\${output}" "\${logfile}"
		sleep 5
		
        tgtmtchval=$(head -n 1 $output)
		
		echo "------------"
		echo $srcmtchval
		echo $tgtmtchval
		echo "------------"
	    
		
        if [ "$srcmtchval" == "$tgtmtchval" ]
        then
                writelog $logfile INFO $scriptname "Source value $matchsrc and Target value $matchtgt match for reference id: $refid"
				query="insert into table "$auddbname"."$parResTblName" select $refid,\""$matchsrc-$matchtgt"\",\""$srcmtchval"\",\""$tgtmtchval"\",\"Success\",current_timestamp();"
                fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
        else
                writelog $logfile ERROR $scriptname "Validation Failed: Source value $matchsrc and Target value $matchtgt do not match for reference id: $refid"
                #insert code for alerting here. pending
                query="insert into table "$auddbname"."$parResTblName" select $refid,\""$matchsrc-$matchtgt"\",\""$srcmtchval"\",\""$tgtmtchval"\",\"Failure\",current_timestamp();"
                fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
				#exit 1
        fi
}

fnAWSCpToLcl()
{
        srcfile=$1
        tgtloc=$3
        logfile=$2
        encalg=$parEncAlg
        scriptname=`basename "$0"`

        echo "$srcfile"
		echo "$tgtloc"
		echo "$logfile"

        #aws s3 cp --sse "$encalg" "${srcfile}" "${tgtloc}" 
		hadoop fs -get "${srcfile}" "${tgtloc}"

        if [ $? -ne 0 ]
        then
                writelog $logfile ERROR $scriptname "Failure copying AWS file $srcfile to $tgtloc"
                #insert code for alerting here. pending
                exit 1
        else
                writelog $logfile INFO $scriptname ""
        fi

}

fnLogFileColumnCnt()
{
        dt=`date '+%d_%m_%Y.%H_%M_%S'`
        auddbname=$parDbname
        audtable=$parAudTblName
        refid=$1
        datasetid=$2
        fileloc=$3
        filename=$4
        delimiter=$5
        logfile=$6
        chktype=$7
        scriptname=`basename "$0"`

        colcnt=`awk -f '$delimiter' '{print NF; exit}' $fileloc/$filename`

        if [ $? -ne 0 ]
        then
                writelog $logfile ERROR $scriptname "Failure when checking the column count for $fileloc/$filename"
                #insert code for alerting here. pending
                exit 1
        else
                writelog $logfile INFO $scriptname "Number of columns in file $fileloc/$filename is $colcnt"
        fi
}

fnSendNotification()
{
        eval mailaddr=$parEmailAddr
        eval subject=$1
        eval body=$2
        eval attchfilepath=$3
        logfile=$4
		
        echo $body | mailx -s "${subject}" -a "${attchfilepath}" "$mailaddr"

        if [ $? -ne 0 ]
        then
                writelog $logfile ERROR $scriptname "Failure sending email. Check /var/log/maillog"
                exit 1
        else
                writelog $logfile INFO $scriptname "Email sent to $mailaddr with subject $subject"
        fi
}

fnFetchFileColumnCount()
{
        refid=$1
        datasetid=$2
        path=$3
        cnttblfilename=$4
        delimiter=$5
        headerrecord=$6
        recordminlength=$7
        chktype=$8
		numberofcolumns=$9
        auddbname=$parDbname
        audtable=$parAudTblName
echo "NO OF COLUMNS RECIEVED $numberofcolumns"
echo "HEADER RECORD RECIEVED $headerrecord"
echo "DELIMITER RECIEVED $delimiter"

        if [ "$delimiter" = "PIPE" ] || [ "$delimiter" = "|" ]
        then
                        colcnt=$(java -jar $jarname GetRecordCount "$headerrecord" "$delimiter")
        elif [ "$delimiter" = "TAB" ] || [ "$delimiter" = "\t" ]
        then
                        colcnt=$(java -jar $jarname GetRecordCount "$headerrecord" "$delimiter")
        elif [ "$delimiter" = "COMMA" ] || [ "$delimiter" = "," ]
        then
    
                        colcnt=$(java -jar $jarname GetRecordCount "$headerrecord" "$delimiter")
        elif [ "$delimiter" = "NO DELIMITER" ] || [ "$delimiter" = "NO DEMILITER" ]
        then
                        record_length=`echo ${#headerrecord}`
                        metadata_length=`echo ${array[8]} | cut -d'-' -f1`

                        echo "record_length is $record_length"
                        echo "metadata_lenght is $metadata_length"

                if [ "$record_length" -ge "$metadata_length" ]
                then
                                #colcnt=$metadata_length
				colcnt=$numberofcolumns
                                echo "THE colcnt is $colcnt"
                else
                                colcnt="0"
                fi

        fi

    output=$log/insert_aud_"$refid"_dt.out
    query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$path"\",\""$cnttblfilename"\",\""$chktype"\",\""$colcnt"\",current_timestamp();"
    fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
}
fnGetSASCount(){

        tablename=$1
        sasfilename=$2
        datasetid=$3
        refid=$4
        path=$5
        cnttblfilename=$6
        logfile=$7
        sas_index_fix=$8

        auddbname=$parDbname
        audtable=$parAudTblName
		
		
        sas_details=$(cat $sasfilename | grep "${tablename}")
		
	

        if [ "$sas_details" == "" ]
                then
                        writelog $logfile ERROR $scriptname "Error Occured with SAS extract file for $tablename"
        fi

        sas_column_count="$(echo $sas_details |cut -d',' -f3)"
	sas_column_count=$(echo "$sas_column_count" | tr -dc '0-9')
		
		#echo "THE SAS COLUMN COUNT IS $sas_column_count"
        
		if [ "$sas_column_count" == "" ]
                then
                        writelog $logfile ERROR $scriptname "Error Occured with SAS extract file for $tablename"
        fi

        sas_record_count="$(echo $sas_details |cut -d',' -f2)"
		
        if [ "$sas_column_count" == "" ]
                then
                        writelog $logfile ERROR $scriptname "Error Occured with SAS extract file for $tablename"
        fi


        sas_column_count=`expr ${sas_column_count} - 2`

		if [ "$sas_index_fix"  == "" ]
		then
				sas_index_fix=0
		fi

		sas_column_count=`expr ${sas_column_count} - ${sas_index_fix}`

        output=$log/sas_"$refid"_impala.out
        chktype="SASRECORDCOUNT"
        query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$path"\",\""$cnttblfilename"\",\""$chktype"\",\""$sas_record_count"\",current_timestamp();"
        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

        output=$log/sas_"$refid"_impala.out
        chktype="SASCOLUMNCOUNT"
        query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$path"\",\""$cnttblfilename"\",\""$chktype"\",\""$sas_column_count"\",current_timestamp();"
        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

        rm $output
	rm -r $parTmpLoc/$refid
		
		
		
}

fnCheckTableLocation(){
                refid=$1
                databasename=$2
				tablename=$3
				s3location=$4
                logfilename=$5
                datasetid=$6
                chktype=$7
				dataset_name=$8
				dt2_status_table=$9
				aud_db=$parDbname
				
                scriptname=`basename "$0"`

                output=$log/table_location_out_$refid
                query="describe formatted $databasename.$tablename ;"
                fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
                tablocation=$(cat $output | grep 'Location' | cut -d'|' -f2)
                echo $tablocation
                echo $s3location

                IFS='/' read -r -a patharray <<< "$s3location"
                protocal=${patharray[0]}
                Bucket=${patharray[2]}
                Zone=${patharray[3]}
                Secutity=${patharray[4]}
                sourceqal1=${patharray[6]}
                datefield=${patharray[7]}
                sourceqal2=${patharray[8]}

                fmonth=${datefield:0:2}
                fday=${datefield:2:2}
                fyear=${datefield:4:4}
		
		
		
		
		output=$log/rec_insert_date_$refid.out
		query="select max(rec_insert_date) from $aud_db.$dt2_status_table where upper(status)=\"DI STARTED\" and refid=$refid;"
		
        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
		di_insert_date=`cat $log/rec_insert_date_$refid.out`
		
		rec_insert_date=`date --date "$di_insert_date" +%s | awk '{ print strftime("%Y%m%d", $1);}'`
		
		
		tablocation=$(echo $tablocation |sed 's/s3a/s3/g' )
		
		
                Bucket=$(echo $Bucket | sed 's/-landing-/-raw-/g')
                compare_location="$protocal//$Bucket/Raw/$Secutity/$dataset_name/$rec_insert_date/$tablename"

						

                if [ "${tablocation}" == "${compare_location}" ]
                then
                                writelog "${logfilename}" "INFO" "${scriptname}"  "External Table location verified <$tablocation , $compare_location>, PASS"

                                query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$compare_location"\",\""$tablename"\",\""$chktype"\",\"PASS\",current_timestamp();"
                                fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
								
								sleep 20
								query="insert into table "$auddbname"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Success\",current_timestamp();"
								fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

                else
                                writelog "${logfilename}" "ERROR" "${scriptname}"  "External Table location verified <$tablocation , $compare_location>, FAILURE"

                                query="insert into table "$auddbname"."$audtable" select $refid,\""$datasetid"\",\""$compare_location"\",\""$tablename"\",\""$chktype"\",\"FAILURE\",current_timestamp();"
                                fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
								
								sleep 20
								query="insert into table "$auddbname"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Failure\",current_timestamp();"
								fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

                fi

                rm $output
}
fnSASImpalaComparision()
{
			db_name=$1
			table_name=$2
			tableid=$3
			logfile=$4
			AWS_SAS_Location=$5
			raw_table_name=$6
			meta_database_name=$7
			aws_sas_temp=$8
			refid=$9
			datasetid=${10}
			chktype=${11}
			cnttblfilename=${12}
 			audtable=$parAudTblName
					
			output=$log/impalaheader.out
			
			
			Impala_SAS_File_Location="${AWS_SAS_Location}${meta_database_name^^}/$cnttblfilename.csv"
				
			echo $Impala_SAS_File_Location
			fnAWSCpToLcl ${Impala_SAS_File_Location} $logfile $parTmpLoc
			Impala_Dump_File_Location="$aws_sas_temp/$cnttblfilename.csv"

			Header=`awk 'NR==1' ${Impala_Dump_File_Location}`
			Splitting_Header=`echo $Header | awk -F\' '{for(i=1;i<=NF;i++)if(i%2==0){print $i}}'`
			echo "$Splitting_Header"
			arr1=(${Splitting_Header//,/ })
			
			querySASColumnName="select sas_column_name from $db_name.$table_name where tableid="$tableid" ORDER BY column_id;"
			echo "$querySASColumnName"
			fnExecImpalaShell q "\${querySASColumnName}" "\${output}" "\${logfile}"
			echo "$output"
			output_sql=`cat $log/impalaheader.out`
			echo "$output_sql"
			
			arr2=(${output_sql//,/ })
			A=${arr1[@]};
			B=${arr2[@]};
			
			if [ "$A" == "$B" ] ; then
					writelog $logfile INFO $scriptname "Impala column names and  SAS dump file column names are matching"
					
					query="insert into table "$db_name"."$audtable" select $refid,\""$datasetid"\",\""$AWS_SAS_Location"\",\""$cnttblfilename"\",\""$chktype"\",\"PASS\",current_timestamp();"
                   			 fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
					
					sleep 20
					query="insert into table "$db_name"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Success\",current_timestamp();"
					fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
					
					rm $output
					rm $aws_sas_temp/$cnttblfilename.csv
					rm $log/impalaheader.out
			else
					writelog $logfile ERROR $scriptname "Impala column names and  SAS dump file column names are not matching"
					
					query="insert into table "$db_name"."$audtable" select $refid,\""$datasetid"\",\""$AWS_SAS_Location"\",\"" $cnttblfilename"\",\""$chktype"\",\"FAIL\",current_timestamp();"
                    			fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
					
					sleep 20
                                        query="insert into table "$db_name"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Failure\",current_timestamp();"
                                        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

					rm $output
					rm $aws_sas_temp/$cnttblfilename.csv
                                        rm $log/impalaheader.out

					exit 1
			fi
} 

fnDataTypeComparision()
{
                        
                        tableid=$1
                        logfile=$2
                        meta_database_name=$3
			column_defination_table=$4
			delimiter=$5
			second_rec=$6
                        refid=$7
			chktype=$8
			datasetid=$9
			cnttblfilename=${10}
			audtable=$parAudTblName
						
			output=$log/column_datatype.out
											
                       queryColumnDataType="select column_datatype,column_position,date_format from "$meta_database_name"."$column_defination_table" where tableid=$tableid and upper(column_name) <> \"ORACLE_ID\" order by column_id;" 

								
			fnExecImpalaShell q "\${queryColumnDataType}" "\${output}" "\${logfile}"
						
												
			outputstring=$(java -jar $jarname DataTypeChecker $second_rec $output $delimiter)
						   
			errorcheck=$(echo $outputstring | grep "ERROR")
						 
			 if [ "$errorcheck" == "" ] ; then
			 writelog $logfile INFO $scriptname "DataType Comparison Passed"
					
			query="insert into table "$meta_database_name"."$audtable" select $refid,\""$datasetid"\",\"""\",\""$cnttblfilename"\",\""$chktype"\",\"PASS\",current_timestamp();"
                   	fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
			
			sleep 20                       
			query="insert into table "$meta_database_name"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Success\",current_timestamp();"
                        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
			
			rm $output

			else 
			writelog $logfile INFO $scriptname "DataType Comparison Failed"
							 
			query="insert into table "$meta_database_name"."$audtable" select $refid,\""$datasetid"\",\"""\",\""$cnttblfilename"\",\""$chktype"\",\"FAIL\",current_timestamp();"
                   	fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
			
			sleep 20                        
			query="insert into table "$meta_database_name"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Failure\",current_timestamp();"

			rm $output
                        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

                        
                        fi
}

fnSASRecCountDiff()
{
	table_name=$1
	refid=$2
	chktype=$3
	datasetid=$4
	
	
	output=$log/sas_refresh_diff.out 

querySASRefresh="select chk_value from "$parDbname"."$parAudTblName" where cnttblfilename=\""$table_name"\" and chk_type=\""SASRECORDCOUNT"\" order by rec_insert_date limit 2"

fnExecImpalaShell q "\${querySASRefresh}" "\${output}" "\${logfile}"

count=`cat $log/sas_refresh_diff.out`

echo "THE SAS RECORD COUNTS DIFFERENCE $count"

val1=`echo $count | cut -d' ' -f1`
val2=`echo $count | cut -d' ' -f2`

echo "The value of value1 is $val1"
echo "The value of value2 is $val2"
			
			if [ "$val1" == "$val2" ] ; then
					writelog $logfile INFO $scriptname "Impala SAS Refresh is difference is zero"
					
					query="insert into table "$parDbname"."$parAudTblName" select $refid,\""$datasetid"\",\"""\",\""$table_name"\",\""$chktype"\",\""0%"\",current_timestamp();"
                   			 fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
					
					sleep 20
					query="insert into table "$parDbname"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Success\",current_timestamp();"
					fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
					
					rm $output
					rm $log/sas_refresh_diff.out
			else 
				echo "The value of val1 in else $val1"
				echo "The value of val2 in else $val2"
				Diff=`echo "$val1" "$val2" | awk '{print ($1-$2)/($1+$2/2)*100}'`
				new_diff="$Diff%"
				writelog $logfile ERROR $scriptname "Impala SAS Refresh is different"
					
					query="insert into table "$parDbname"."$parAudTblName" select $refid,\""$datasetid"\",\"""\",\""$table_name"\",\""$chktype"\",\""$new_diff"\",current_timestamp();"
                   			 fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"
					
					sleep 20
                                        query="insert into table "$parDbname"."$parResTblName" select $refid,\""$chktype"\",\"""\",\"""\",\"Failure\",current_timestamp();"
                                        fnExecImpalaShell q "\${query}" "\${output}" "\${logfile}"

					rm $output
					rm $log/sas_refresh_diff.out

					exit 1
			fi
}

