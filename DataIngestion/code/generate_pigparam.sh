#*****************************************************
#This script is used to generate the parameter file
#for the tableid passed as parameter that will further
#serve as input to the Pig script
#Author : Swati Madan swmadan@deloitte.com
#*****************************************************

#!/bin/bash

#input parameter is the tableid
tableid=$1
input_dir=$2
output_dir=$3
#metadata database
CONF_FILE=$4
. $CONF_FILE


hive -S -e "SELECT * FROM "$md_db_name".column_definition where tableid = "$tableid" order by TABLEID,column_id;"  | sed 's/[\t]/|/g' > ColumnDefinition.txt
hive -S -e "SELECT HEADER_COUNT FROM "$md_db_name".table_definition where tableid = "$tableid";"   | sed 's/[\t]/|/g' > HeaderCount.txt

if [ `wc -l HeaderCount.txt|cut -d" " -f1` -ge "2" ]; then
   echo "There is more than 1 record in table_definition table for the tableid"
   exit 1
elif [ `cat HeaderCount.txt` == "1" ]; then
   header_option="SKIP_HEADER"
else
   header_option=""
fi

count=0
IFS='|'

while read tableid column_id column_name column_description column_datatype column_position X
       do
          count=`expr $count + 1`
          col_name=`echo $column_name| tr -s " "|sed 's/[ ]//'`
          col_pos=`echo $column_position| tr -s " "|sed 's/[ ]//'`
          if [ $count -eq 1 ]; then
             pos_parm=$col_pos
             field_list=$col_name": CHARARRAY"
          else
             pos_parm=$pos_parm", "$col_pos
             field_list=$field_list", "$col_name": CHARARRAY"
          fi

        done <ColumnDefinition.txt

echo "jar_file = '<jar file location>'" > dynmParam.param
echo "input_dir = '"$input_dir"'" >> dynmParam.param
echo "pos_param = '"$pos_parm"'" >> dynmParam.param
echo "header_option = '"$header_option"'" >> dynmParam.param
echo "field_list = '"$field_list"'" >> dynmParam.param
echo "output_dir = '<output-directory>'" >> dynmParam.param
echo "output_delim = '^|'" >> dynmParam.param
echo "multi_line = 'YES_MULTILINE'" >> dynmParam.param
echo "os_option = 'UNIX'" >> dynmParam.param
exit 0

