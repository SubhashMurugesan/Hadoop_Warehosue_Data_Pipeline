set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing = true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
create table if not exists Project_1.tgt_warehouse_cust(
custid int,
username string,
quote_count string,
ip string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
day int,
updated_time string)
PARTITIONED BY (year String,month String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as orc
TBLPROPERTIES ('transactional'='true');
insert overwrite table Project_1.tgt_warehouse_cust(select st.custid,st.username,st.quote_count,st.ip,st.prp_1,st.prp_2,st.prp_3,st.ms,
st.http_type,st.purchase_category,st.total_count,
st.purchase_sub_category,st.http_info,st.status_code,
st.day,st.updated_time,st.month,st.year from Project_1.cust_stagging st
full outer join Project_1.tgt_warehouse_cust tgt on tgt.custid=st.custid
where st.custid is not null
union
select tgt1.custid,tgt1.username,tgt1.quote_count,tgt1.ip,tgt1.prp_1,tgt1.prp_2,tgt1.prp_3,tgt1.ms,
tgt1.http_type,tgt1.purchase_category,tgt1.total_count,
tgt1.purchase_sub_category,tgt1.http_info,tgt1.status_code,
tgt1.day,tgt1.updated_time,tgt1.month,tgt1.year from Project_1.cust_stagging st1
full outer join Project_1.tgt_warehouse_cust tgt1 on tgt1.custid=st1.custid
where st1.custid is null);
