#!/bin/bash

#command to create table in mysql

mysql --user="root" --password="Welcome@123" --database="purchase_data" --execute="create table if not exists shoop_data(
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
--connect jdbc:mysql://localhost:3306/purchase_data?useSSL=False \
--username root --password Welcome@123 \
--query 'SELECT * from shoop_data  WHERE $CONDITIONS' \
--split-by custid \
--target-dir /user/saif/HFS/Output/Project_1/imp_data


#hdfs to hive table
hive -f '/home/saif/Desktop/Project_1/scripts/hive.hql'
hive -f '/home/saif/Desktop/Project_1/scripts/hive_tb1.hql'
hive -f '/home/saif/Desktop/Project_1/scripts/hive_tb2.hql'
hive -f '/home/saif/Desktop/Project_1/scripts/hive_tb3.hql'


mysql --local-infile=1 -uroot -pWelcome@123 -e "truncate table purchase_data.reconcilation_tbl;"

sqoop export --connect jdbc:mysql://localhost:3306/purchase_data?useSSL=False \
--username root --password Welcome@123 \
--table reconcilation_tbl \
--export-dir /user/hive/warehouse/project_1.db/cust_stagging \
--input-fields-terminated-by ','
