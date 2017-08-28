#!/bin/bash
#   DI Automation Event Handler Script
#	Event handler to identify new request from Data-Transport API and trigger DI
#	Author: Siddharth Sharma     
#		                                              
#       $1 = properties file name passed with parameters   

CONF_FILE=$1
. $CONF_FILE


#below kinit commnd is added to handel the cron job issue for kerberos
#kinit svc_dikudataload@CELGENE.COM -kt /home/svc_dikudataload/svc_dikudataload.keytab
#if [ $? != 0 ]
#then
#        echo "Kinit ran succesfully"
#else
#        echo "Kinit was not succesfull "
#fi


if [ ! -d "$RUNFILE" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir -p $RUNFILE
fi

scriptrunning_ack=0
scriptrunning_ack=`ls $RUNFILE/*.ack|wc -l`

if [ $scriptrunning_ack -eq 0 ]; then
	DT=`date +%Y%m%d%H%M%S`
	touch $RUNFILE/$DT.ack
	sh $E2EPATH/request_ack.sh $CONF_FILE &
fi

scriptrunning=0
scriptrunning=`ls $RUNFILE/*.txt|wc -l`

if [ $scriptrunning -eq 0 ]; then
	DT=`date +%Y%m%d%H%M%S`
	touch $RUNFILE/$DT.txt
	sh $E2EPATH/DIEventHandler.sh $CONF_FILE &
fi


