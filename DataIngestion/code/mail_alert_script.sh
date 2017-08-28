#!/bin/bash

#NOW=$(date +"%Y_%m_%d")
NOW=`date  +'%m_%d_%Y_%T'`

TO_ADDRESS_LIST=$1
SPACE=" "
DATASETNAME=$4
TABLENAME=$5
UNDERSCORE="_"
ALERT="alert"
LOGPATH=$6
LOGFILE="$DATASETNAME$UNDERSCORE$TABLENAME$UNDERSCORE$ALERT$UNDERSCORE$NOW.log"
LOG_FILE_PATH="alert_$NOW.log"
LOG_FILE_PATH_ERROR="$LOGPATH$LOG_FILE_PATH"

COMPLETE_LOG_FILE_PATH="$LOGPATH$LOGFILE"


#if there are multiple destinations(To:Locations)
IFS=';' read -ra ADDR <<< "$TO_ADDRESS_LIST"
for i in "${ADDR[@]}"; do
    # process "$i"
        #echo "$i"
        TO_ADDRESS=$TO_ADDRESS$SPACE$i
done


#From address is hard coded
#FROM="user@domain.com"




if [ $# -eq 6 ];then

        #Displaying parameters passed on console
        #echo "$NOW:From Address is :$FROM" >> $COMPLETE_LOG_FILE_PATH
        echo "$NOW:To Address is :$1"  >> $COMPLETE_LOG_FILE_PATH
        echo "$NOW:Subject of mail is:$2" >> $COMPLETE_LOG_FILE_PATH
        echo "$NOW:Path of file having message body:$3" >> $COMPLETE_LOG_FILE_PATH
	echo "$NOW:Dataset:$4" >> $COMPLETE_LOG_FILE_PATH
        echo "$NOW:Table Name is:$5" >> $COMPLETE_LOG_FILE_PATH


        echo "$NOW:The send mail command to be executed is" >> $COMPLETE_LOG_FILE_PATH
        echo "$NOW:echo | mailx -s \"$2\"  -q $3 $TO_ADDRESS" >> $COMPLETE_LOG_FILE_PATH

        #echo "$NOW:Mail  Alert Initiated......" >> $COMPLETE_LOG_FILE_PATH
        echo | mailx -s "$2" -q $3 $TO_ADDRESS >> $COMPLETE_LOG_FILE_PATH
        #echo `cat $3`|mail -s \"$2\" $TO_ADDRESS

        if [ $? == 0 ];then
                echo "$NOW:Mail Alert Initiated..." >> $COMPLETE_LOG_FILE_PATH
        else
                echo "$NOW:Mail Sending Failure..." >> $LOG_FILE_PATH_ERROR
                echo "$NOW:Check path /var/log/maillog for Failed Reasons..." >> $LOG_FILE_PATH_ERROR
        fi
else
        #If Number parameters entered mismatch error info present in  LOG_FILE_PATH_ERROR
        echo "$NOW:Incorrect number of arguments passed to the script!!!" >> $LOG_FILE_PATH_ERROR
	#Displaying parameters passed on console
        #echo "$NOW:From Address is :$FROM" >> $LOG_FILE_PATH_ERROR
        echo "$NOW:To Address is :$1"  >> $LOG_FILE_PATH_ERROR
        echo "$NOW:Subject of mail is:$2" >> $LOG_FILE_PATH_ERROR
        echo "$NOW:Path of file having message body:$3" >> $LOG_FILE_PATH_ERROR

        echo "$NOW:The send mail command to be executed is" >> $LOG_FILE_PATH_ERROR
        echo "$NOW:echo | mailx -s \"$2\"  -q $3 $TO_ADDRESS" >> $LOG_FILE_PATH_ERROR
	echo "$NOW:Dataset:$4" >> $LOG_FILE_PATH_ERROR
        echo "$NOW:Table Name is:$5" >> $LOG_FILE_PATH_ERROR
	echo "$NOW:LOG Path  is:$6" >> $LOG_FILE_PATH_ERROR




fi
