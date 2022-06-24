#!/bin/bash

#bringing the param file:
. /home/saif/Desktop/Project_1/env/jdbc_mysql_credential.prm

#log file detials
LOG_DIR=/home/saif/Desktop/Project_1/logs
FILE_NAME=`basename $0`
DT=`date '+%Y%m%d_%H:%M:%S'`
LOG_FILE=$LOG_DIR/${FILE_NAME}_${DT}.log

#command to create table in mysql
##validating mysql commands
mysql --user="root" --password="Welcome@123" --database="purchase_data"
--execute="create table if not exists shoop_data(
custid int,
username varchar(30),
quote_count varchar(20),
ip varchar(20),
entry_time varchar(40),
prp_1 int,
prp_2 int,
prp_3 int,
ms  varchar(5),
http_type varchar(5),
purchase_category varchar(50),
total_count int,
purchase_sub_category  varchar(50),
http_info text,
status_code  int,
updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"

#movind daata form local file to mysql
file_lt=`ls /home/saif/Desktop/Project_1/datasets -tp | grep -v /$ | head -1`
echo $file_lt
mysql --local-infile=1 -uroot -pWelcome@123 -e "set global local_infile=1 ;
truncate table purchase_data.shoop_data;
load data local infile '/home/saif/Desktop/Project_1/datasets/${file_lt}' into table purchase_data.shoop_data fields terminated by ',';
update purchase_data.shoop_data set updated_time=CURRENT_TIMESTAMP() WHERE updated_time is NULL;"

##validating mysql commands
if [ $? -eq 0 ]
then echo "mysql successfully executed at ${DT}" >> ${LOG_FILE}
else echo "mysql commands failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


OP_DIR=/user/saif/HFS/Output/Project_1

#Dir Checking whether it exists or not:
hdfs dfs -test -d ${OP_DIR}

#Validation for Dir existence:
if [ $? -eq 0 ]
then
        hdfs dfs -rm -r ${OP_DIR}
        echo "Dir Deleted Successfully"
fi

#scoop import form mysql to hdfs 

sqoop import \
--connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False \
-username ${USERNAME} --password-file ${PASSWORD_FILE} \
--query 'SELECT * from shoop_data  WHERE $CONDITIONS' \
--split-by custid \
--target-dir /user/saif/HFS/Output/Project_1/imp_data



if [ $? -eq 0 ]
then echo "sqoop loading  completed ${DT}" >> ${LOG_FILE}
else echo "sqoop import failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi

#hdfs to hive table
hive -f '/home/saif/Desktop/Project_1/scripts/hive.hql'
hive -f '/home/saif/Desktop/Project_1/scripts/hive_tb1.hql'
hive -f '/home/saif/Desktop/Project_1/scripts/hive_tb2.hql'
hive -f '/home/saif/Desktop/Project_1/scripts/hive_tb3.hql'

mysql --local-infile=1 -uroot -pWelcome@123 -e "truncate table purchase_data.reconcilation_tbl;"

if [ $? -eq 0 ]
then echo "hive scd successfully executed at ${DT}" >> ${LOG_FILE}
else echo "hive scd job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


sqoop export --connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False \
-username ${USERNAME} --password-file ${PASSWORD_FILE} \
--table reconcilation_tbl \
--export-dir /user/hive/warehouse/project_1.db/cust_stagging \
--input-fields-terminated-by ',' 

if [ $? -eq 0 ]
then echo "sqoop exp job successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sqoop exp job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi
